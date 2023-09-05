import json
import threading
import time
from functools import wraps
from warnings import filterwarnings
from datetime import datetime
from hashlib import sha256

from amqpstorm import UriConnection, AMQPConnectionError
from flask import Flask
from retry.api import retry_call


class AmqpService:
    def __init__(self):
        self.connection = None
        self.exchange_name = None
        self.channel = None
        self.mq_url = None
        self.logger = None

    def init_app(self, app: Flask):
        self.mq_url = app.config["MQ_URL"]
        self.logger = app.logger
        self._validate_channel_connection()
        self.exchange_name = app.config["MQ_EXCHANGE"]

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
            queue_arguments=None
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
                            arguments=queue_arguments
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
import json
import threading
import time
from functools import wraps
from warnings import filterwarnings
from datetime import datetime
from hashlib import sha256

from amqpstorm import UriConnection, AMQPConnectionError
from flask import Flask
from retry.api import retry_call


class AmqpService:
    def __init__(self):
        self.connection = None
        self.exchange_name = None
        self.channel = None
        self.mq_url = None
        self.logger = None

    def init_app(self, app: Flask):
        self.mq_url = app.config["MQ_URL"]
        self.logger = app.logger
        self._validate_channel_connection()
        self.exchange_name = app.config["MQ_EXCHANGE"]

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
            queue_arguments=None
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
                            arguments=queue_arguments
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
