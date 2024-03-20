import json
import threading
from os import getenv

from .exchange_params import ExchangeParams
from .queue_params import QueueParams

from amqpstorm import UriConnection, AMQPConnectionError
from datetime import datetime
from functools import wraps
from hashlib import sha256
from retry.api import retry_call
from time import sleep
from typing import Union, List
from warnings import filterwarnings


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
    ):
        self.mq_url = None
        self.mq_exchange = None
        self.logger = None
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
        self._validate_channel_connection()

    def check_health(self, check_consumers=True):
        if not self.get_connection().is_open:
            # try to recover connection before failing the health check
            self._validate_channel_connection(0.1)
            if not self.get_connection().is_open:
                return False, "Connection not open"
        if check_consumers and len(self.channel.consumer_tags) < 1:
            return False, "No consumers available"
        return True, "Connection open"

    def get_connection(self):
        return self.connection

    def _validate_channel_connection(self, retry_delay=5.0, max_retries=20):
        retries = 0
        while (retries <= max_retries) and (
            not self.connection
            or self.get_connection().is_closed
            or self.channel.is_closed
        ):
            try:
                self.connection = UriConnection(self.mq_url)
                self.channel = self.get_connection().channel()
            except Exception as ex:
                retries += 1
                if retries > max_retries:
                    exit(0)

                self.logger.warning(
                    f"An error occurred while connecting to {self.mq_url}: {str(ex)}"
                )
                self.logger.warning(f"Reconnecting in {retry_delay} seconds...")
                sleep(retry_delay)

    def send(
        self,
        body,
        routing_key: str,
        exchange_type: str = "topic",
        retries: int = 5,
        message_version: str = "v1.0.0",
        debug_exchange: bool = False,
        **properties,
    ):
        filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)
        exchange_name = (
            f"{self.mq_exchange}-development" if self.development else self.mq_exchange
        )
        self._validate_channel_connection()
        self.channel.exchange.declare(
            exchange=f"{exchange_name}-debug" if debug_exchange else exchange_name,
            exchange_type=exchange_type,
            passive=self.exchange_params.passive,
            durable=self.exchange_params.durable,
            auto_delete=self.exchange_params.auto_delete,
        )

        retry_call(
            self._publish_to_channel,
            (body, routing_key, message_version, debug_exchange),
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
        exchange_name = (
            f"{self.mq_exchange}-development" if self.development else self.mq_exchange
        )

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
            @wraps(f)
            def new_consumer():
                queue = f.__name__.replace("_", getenv("MQ_DELIMITER", ".")) if queue_name is None else queue_name
                retries = 0
                while retries <= max_retries:
                    try:
                        self._validate_channel_connection()
                        self.channel.exchange.declare(
                            exchange_name if exchange_name else self.mq_exchange,
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
                            cb_function, queue=queue, no_ack=self.queue_params.no_ack if auto_ack is None else auto_ack
                        )
                        self.channel.queue.bind(
                            queue=queue,
                            exchange=self.mq_exchange,
                            routing_key=routing_key,
                        )
                        self.logger.info(f"Start consuming queue {queue}")
                        self.channel.start_consuming()
                    except Exception as ex:
                        retries += 1
                        if retries > max_retries:
                            exit(0)

                        self.logger.exception(
                            "An error occurred while consuming queue %s: %s",
                            queue,
                            ex,
                        )
                        self.logger.warning(f"Retrying in {retry_delay} seconds...")
                        sleep(retry_delay)

            thread = threading.Thread(target=new_consumer)
            thread.daemon = True
            thread.start()

            return f

        return decorator
