import json
import os
import threading

from amqpstorm import UriConnection, AMQPConnectionError
from datetime import datetime
from exchange_params import ExchangeParams
from functools import wraps
from hashlib import sha256
from queue_params import QueueParams
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
    ):
        self.mq_url = app.config["MQ_URL"]
        self.mq_exchange = app.config["MQ_EXCHANGE"]
        self.logger = app.logger
        self.body_parser = body_parser
        self.msg_parser = msg_parser
        self._validate_channel_connection()

    def check_health(self, check_consumers=True):
        if not self.get_connection().is_open:
            raise Exception("Connection not open")
        if check_consumers and len(self.channel.consumer_tags) < 1:
            raise Exception("No consumers available")
        return True, "healthy"

    def get_connection(self):
        return self.connection

    def _validate_channel_connection(self, retry_delay=5, max_retries=20):
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
        **properties,
    ):
        filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)
        exchange_name = (
            f"{self.mq_exchange}-debug" if self.development else self.mq_exchange
        )
        self._validate_channel_connection()
        self.channel.exchange.declare(
            exchange=exchange_name,
            exchange_type=exchange_type,
            passive=self.exchange_params.passive,
            durable=self.exchange_params.durable,
            auto_delete=self.exchange_params.auto_delete,
        )

        retry_call(
            self._publish_to_channel,
            (body, routing_key, message_version, self.development),
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
        if "message_id" not in properties:
            properties["message_id"] = sha256(
                json.dumps(body).encode("utf-8")
            ).hexdigest()
        if "timestamp" not in properties:
            properties["timestamp"] = int(datetime.now().timestamp())

        if "headers" not in properties:
            properties["headers"] = {}
        properties["headers"]["x-message-version"] = message_version
        filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)
        exchange_name = (
            f"{self.mq_exchange}-debug" if debug_exchange is True else self.mq_exchange
        )
        self._validate_channel_connection()
        self.channel.basic.publish(
            exchange=exchange_name,
            routing_key=routing_key,
            body=bytes(json.dumps(body), "utf-8"),
            properties=properties,
        )

    def queue(
        self,
        routing_key: Union[str, List[str]],
        exchange_type: str = "topic",
        auto_ack: bool = False,
        dead_letter_exchange: bool = False,
        props_needed: List[str] | None = None,
        exchange_name: str = None,
        max_retries: int = 5,
        retry_delay: int = 5,
        queue_arguments: dict = None,
    ):
        if queue_arguments is None:
            queue_arguments = {"x-queue-type": "quorum"}

        def decorator(f):
            @wraps(f)
            def new_consumer():
                queue_name = f.__name__.replace("_", os.getenv("MQ_DELIMITER", "."))
                retries = 0
                while retries <= max_retries:
                    try:
                        self._validate_channel_connection()
                        self.channel.exchange.declare(
                            exchange_name if exchange_name else self.mq_exchange,
                            exchange_type=exchange_type,
                            durable=self.exchange_params.durable,
                            passive=self.exchange_params.passive,
                            auto_delete=self.exchange_params.auto_remove,
                        )
                        self.channel.queue.declare(
                            queue=queue_name,
                            durable=self.queue_params.durable,
                            passive=self.queue_params.passive,
                            auto_delete=self.queue_params.auto_delete,
                            arguments=queue_arguments,
                        )
                        self.channel.basic.qos(prefetch_count=1)
                        self.channel.basic.consume(
                            f, queue=queue_name, no_ack=self.queue_params.no_ack
                        )
                        self.channel.queue.bind(
                            queue=queue_name,
                            exchange=self.mq_exchange,
                            routing_key=routing_key,
                        )
                        self.logger.info(f"Start consuming queue {queue_name}")
                        self.channel.start_consuming()
                    except Exception as ex:
                        retries += 1
                        if retries > max_retries:
                            exit(0)

                        self.logger.warning(
                            f"An error occurred while consuming the queue {queue_name}: {str(ex)}"
                        )
                        self.logger.warning(f"Retrying in {retry_delay} seconds...")
                        sleep(retry_delay)

            thread = threading.Thread(target=new_consumer)
            thread.daemon = True
            thread.start()

            return f

        return decorator
