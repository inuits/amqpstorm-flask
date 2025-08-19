import json
import logging
import sys
import threading
from os import getenv

from .exchange_params import ExchangeParams
from .queue_params import QueueParams

from amqpstorm import UriConnection, AMQPConnectionError
from datetime import datetime
from functools import wraps
from hashlib import sha256
from retry.api import retry_call
from time import sleep, time
from typing import Union, List
from warnings import filterwarnings
from apscheduler.schedulers.background import BackgroundScheduler


class RabbitMQ:
    def __init__(
            self,
            app=None,
            queue_prefix=None,
            body_parser=None,
            msg_parser=None,
            queue_params=None,
            development=None,
            on_message_error_callback=None,
            middlewares=None,
            exchange_params=None,
            *,
            default_send_properties=None,
            mq_url=None,
            mq_exchange=None,
            logger=None
    ):
        self.mq_url = mq_url
        self.mq_exchange = mq_exchange
        self.logger = logger
        self.body_parser = body_parser
        self.msg_parser = msg_parser
        self.exchange_params = exchange_params or ExchangeParams()
        self.queue_params = queue_params or QueueParams()
        if app is not None:
            self.init_app(
                app,
                body_parser=body_parser,
                msg_parser=msg_parser,
            )
        self.connection = None
        self.channel = None
        self.json_encoder = None
        self.development = development if development is not None else False
        self.last_message_consumed_at = 0
        self.scheduler = BackgroundScheduler()

    def init_app(
            self,
            app,
            queue_prefix=None,
            body_parser=None,
            msg_parser=None,
            development=None,
            on_message_error_callback=None,
            middlewares=None,
            json_encoder=None
    ):
        self.mq_url = app.config.get("MQ_URL") or getenv("MQ_URL")
        self.mq_exchange = app.config.get("MQ_EXCHANGE") or getenv("MQ_EXCHANGE")
        self.logger = app.logger
        self.body_parser = body_parser
        self.msg_parser = msg_parser
        self.json_encoder = json_encoder
        if int(getenv("AMQP_STORM_APSCHEDULER", 1))== 1:
            if int(getenv("FILTER_LOGS", 1)) == 1:
                # some logs are useless, but we don't want to fully block a log level
                class LogFilterAPScheduler(logging.Filter):
                    def filter(self, record):
                        message = record.getMessage()
                        return "amqp_consumer_job_" not in message and "_validate_channel_connection" not in message
                logging.getLogger("apscheduler.scheduler").addFilter(LogFilterAPScheduler())
                logging.getLogger("apscheduler.executors.default").addFilter(LogFilterAPScheduler())
                class LogFilterAmqpStorm(logging.Filter):
                    def filter(self, record):
                        message = record.getMessage()
                        # this logs a warning but also a stacktrace about connection error, we already have logs for that,
                        # no need to log it again with a stacktrace
                        return "Stopping inbound thread due to" not in message
                logging.getLogger("amqpstorm.io").addFilter(LogFilterAmqpStorm())
            self.scheduler.start()
            self._validate_channel_connection()
            self.scheduler.add_job(self._validate_channel_connection, "interval", seconds=5, max_instances=1)
        else:
            if not getenv("WERKZEUG_RUN_MAIN"):
                self._validate_channel_connection()



    def check_health(self, check_consumers=True):
        if not self.get_connection().is_open:
            return False, "Connection not open"
        if check_consumers and len(self.channel.consumer_tags) < 1:
            return False, "No consumers available"
        return True, "Connection open"

    def get_connection(self):
        return self.connection

    def _validate_channel_connection(self):
        max_consumer_idle_time = int(getenv("MQ_MAX_CONSUMER_IDLE_TIME", 300))
        consumed_seconds_ago = (time() - self.last_message_consumed_at)
        if (not self.connection or self.get_connection().is_closed or self.channel.is_closed or
                self.last_message_consumed_at == -1 or (self.last_message_consumed_at != 0 and (consumed_seconds_ago > max_consumer_idle_time))):
            try:
                self.connection = UriConnection(self.mq_url)
                self.channel = self.get_connection().channel()
                self.last_message_consumed_at = 0
            except BaseException as ex:
                if int(getenv("AMQP_STORM_APSCHEDULER", 1)) == 0:
                    sleep(1)
                    self._validate_channel_connection()
                self.logger.error(
                    f"An error occurred while renewing rabbit connection: {str(ex)}"
                )

    def send(
            self,
            body,
            routing_key: str,
            exchange_type: str = "topic",
            retries: int = 5,
            message_version: str = "v1.0.0",
            debug_exchange: bool = False,
            exchange_name: str = None,
            **properties,
    ):
        filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)
        exchange_name = self.mq_exchange if exchange_name is None else exchange_name
        exchange = (
            f"{exchange_name}-development" if self.development else exchange_name
        )
        self._validate_channel_connection()
        self.channel.exchange.declare(
            exchange=f"{exchange}-debug" if debug_exchange else exchange,
            exchange_type=exchange_type,
            passive=self.exchange_params.passive,
            durable=self.exchange_params.durable,
            auto_delete=self.exchange_params.auto_delete,
        )

        retry_call(
            self._publish_to_channel,
            (body, routing_key, message_version, debug_exchange, exchange_name),
            properties,
            exceptions=(AMQPConnectionError, AssertionError),
            tries=retries,
            delay=5,
            jitter=(5, 15),
        )

    def _publish_to_channel(
            self,
            body,
            routing_key: str,
            message_version: str,
            debug_exchange: bool = False,
            exchange_name: str = None,
            **properties,
    ):
        encoded_body = json.dumps(body, cls=self.json_encoder).encode("utf-8")
        if "message_id" not in properties:
            properties["message_id"] = sha256(encoded_body).hexdigest()
        if "timestamp" not in properties:
            properties["timestamp"] = int(datetime.now().timestamp())

        if "headers" not in properties:
            properties["headers"] = {}
        properties["headers"]["x-message-version"] = message_version
        filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)
        self._validate_channel_connection()
        self.channel.basic.publish(
            exchange=f"{exchange_name}-debug" if debug_exchange is True else exchange_name,
            routing_key=routing_key,
            body=encoded_body,
            properties=properties,
        )

    @staticmethod
    def __create_wrapper_function(routing_key, f):
        def wrapper_function(message):
            f(
                routing_key=routing_key,
                body=message.json(),
                message_id=message.message_id,
            )

        return wrapper_function

    def queue(
            self,
            routing_key: Union[str, List[str]],
            exchange_type: str = "topic",
            auto_ack: bool = None,
            dead_letter_exchange: bool = False,
            props_needed: List[str] | None = None,
            exchange_name: str = None,
            max_retries: int = 5,
            retry_delay: int = 5,
            queue_arguments: dict = None,
            prefetch_count: int = 1,
            queue_name: str = None,
            full_message_object: bool = False,
            passive_queue: bool = None
    ):
        if queue_arguments is None:
            queue_arguments = {"x-queue-type": "quorum"}

        def decorator(f):
            queue = f.__name__.replace("_", getenv("MQ_DELIMITER", ".")) if queue_name is None else queue_name

            enabled_queues = None if getenv("MQ_QUEUES") is None else getenv("MQ_QUEUES").split(",")

            if enabled_queues is None or queue in enabled_queues:
                @wraps(f)
                def new_consumer():
                    try:
                        self._validate_channel_connection()
                        self.channel.exchange.declare(
                            exchange=exchange_name if exchange_name else self.mq_exchange,
                            exchange_type=exchange_type,
                            durable=self.exchange_params.durable,
                            passive=self.exchange_params.passive,
                            auto_delete=self.exchange_params.auto_delete,
                        )
                        self.channel.queue.declare(
                            queue=queue,
                            durable=self.queue_params.durable,
                            passive=self.queue_params.passive if passive_queue is None else passive_queue,
                            auto_delete=self.queue_params.auto_delete,
                            arguments=queue_arguments,
                        )
                        self.channel.basic.qos(prefetch_count=prefetch_count)
                        cb_function = f if full_message_object else self.__create_wrapper_function(routing_key, f)
                        self.channel.basic.consume(
                            cb_function, queue=queue,
                            no_ack=self.queue_params.no_ack if auto_ack is None else auto_ack
                        )

                        keys = [routing_key] if isinstance(routing_key, str) else routing_key
                        for key in keys:
                            self.channel.queue.bind(
                                queue=queue,
                                exchange=exchange_name if exchange_name else self.mq_exchange,
                                routing_key=key,
                            )
                        self.logger.info(f"Start consuming queue {queue}")
                        self.channel.start_consuming()
                    except BaseException as ex:
                        if int(getenv("AMQP_STORM_APSCHEDULER", 1)) == 1:
                            self.logger.error(
                                f"An error occurred while consuming queue {queue}: {str(ex)}, apscheduler will try to restart it every 5 seconds"
                            )
                        else:
                            self.logger.error(
                                f"An error occurred while consuming queue {queue}: {str(ex)}, restarting consumer"
                            )
                            sleep(1)
                            new_consumer()

                if int(getenv("AMQP_STORM_APSCHEDULER", 1)) == 1:
                    self.scheduler.add_job(new_consumer, "interval", seconds=5, max_instances=1, name=f"amqp_consumer_job_{f.__name__}")
                else:
                    # Only run the consumer thread in the flask thread that gets reloaded during flask development mode.
                    # Use FORCE_AMQP_STORM_CONSUMER_THREAD=1 if not running in flask development mode, but not recommended,
                    # you should use AMQP_STORM_APSCHEDULER=1 instead.
                    if getenv("WERKZEUG_RUN_MAIN", None) or int(getenv("FORCE_AMQP_STORM_CONSUMER_THREAD", 0)) == 1:
                        thread = threading.Thread(target=new_consumer)
                        thread.daemon = True
                        thread.start()
            return f

        return decorator

    def stop(self):
        self.scheduler.shutdown()
