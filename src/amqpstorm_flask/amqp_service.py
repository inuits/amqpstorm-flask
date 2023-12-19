import json
import os
import threading
import time

from amqpstorm import UriConnection, AMQPConnectionError
from datetime import datetime
from functools import wraps
from hashlib import sha256
from retry.api import retry_call
from warnings import filterwarnings


class RabbitMQ:
    def __init__(
        self,
        app=None,
        queue_prefix="",
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
        self.app = None
        self.config = None
        self.exchange_name = None
        self.mq_url = None
        self.logger = None
        if app is not None:
            self.init_app(
                app,
                queue_prefix,
                body_parser,
                msg_parser,
                development,
                on_message_error_callback,
            )
        self.connection = None
        self.channel = None

    def init_app(
        self,
        app=None,
        queue_prefix="",
        body_parser=None,
        msg_parser=None,
        development=None,
        on_message_error_callback=None,
    ):
        self.app = app
        self.config = app.config
        exchange_name = self.config.get("MQ_EXCHANGE") or os.getenv("MQ_EXCHANGE")
        assert (
            exchange_name
        ), "MQ_EXCHANGE not set. Please define a default exchange name."
        self.exchange_name = exchange_name
        mq_url = self.config.get("MQ_URL") or os.getenv("MQ_URL")
        assert mq_url, "MQ_URL not set. Please define a RabbitMQ url"
        self.mq_url = mq_url
        self.logger = app.logger
        self._validate_channel_connection()

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
                time.sleep(retry_delay)

    def send(
        self,
        body,
        routing_key: str,
        exchange_type,
        retries: int = 5,
        message_version: str = "v1.0.0",
        passive_exchange: bool = True,
        durable_exchange: bool = True,
        debug_exchange: bool = False,
        auto_delete_exchange: bool = False,
        **properties,
    ):
        filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)
        exchange_name = (
            f"{self.exchange_name}-debug"
            if debug_exchange is True
            else self.exchange_name
        )
        self._validate_channel_connection()
        self.channel.exchange.declare(
            exchange=exchange_name,
            exchange_type=exchange_type,
            passive=passive_exchange,
            durable=durable_exchange,
            auto_delete=auto_delete_exchange,
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
            f"{self.exchange_name}-debug"
            if debug_exchange is True
            else self.exchange_name
        )
        self._validate_channel_connection()
        self.channel.basic.publish(
            exchange=exchange_name,
            routing_key=routing_key,
            body=bytes(json.dumps(body), "utf-8"),
            properties=properties,
        )

    def queue_consumer(
        self,
        queue_name,
        routing_key="",
        max_retries=5,
        retry_delay=5,
        exchange_type="topic",
        durable_exchange=True,
        passive_exchange=True,
        durable_queue=True,
        passive_queue=False,
        no_ack=True,
        exchange_name=None,
        auto_delete_exchange=False,
        auto_delete_queue=False,
        queue_arguments=None,
    ):
        if queue_arguments is None:
            queue_arguments = {"x-queue-type": "quorum"}

        def decorator(f):
            @wraps(f)
            def new_consumer():
                retries = 0
                while retries <= max_retries:
                    try:
                        self._validate_channel_connection()
                        self.channel.exchange.declare(
                            exchange_name if exchange_name else self.exchange_name,
                            exchange_type=exchange_type,
                            durable=durable_exchange,
                            passive=passive_exchange,
                            auto_delete=auto_delete_exchange,
                        )
                        self.channel.queue.declare(
                            queue=queue_name,
                            durable=durable_queue,
                            passive=passive_queue,
                            auto_delete=auto_delete_queue,
                            arguments=queue_arguments,
                        )
                        self.channel.basic.qos(prefetch_count=1)
                        self.channel.basic.consume(f, queue=queue_name, no_ack=no_ack)
                        self.channel.queue.bind(
                            queue=queue_name,
                            exchange=self.exchange_name,
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
                        time.sleep(retry_delay)

            thread = threading.Thread(target=new_consumer)
            thread.daemon = True
            thread.start()

            return f

        return decorator

    def check_health(self, check_consumers=True):
        if not self.get_connection().is_open:
            raise Exception("Connection not open")
        if check_consumers and len(self.channel.consumer_tags) < 1:
            raise Exception("No consumers available")
        return True, "healthy"
