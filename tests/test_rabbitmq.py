import threading
import time
from unittest import TestCase
from unittest.mock import MagicMock, patch, PropertyMock


class TestCloseConnection(TestCase):
    def _make_rabbit(self):
        with patch.dict("os.environ", {"MQ_URL": "amqp://localhost", "MQ_EXCHANGE": "test"}):
            from amqpstorm_flask.RabbitMQ import RabbitMQ
            rabbit = RabbitMQ.__new__(RabbitMQ)
            rabbit.connection = None
            rabbit.channel = None
            rabbit.mq_url = "amqp://localhost"
            rabbit.mq_exchange = "test"
            rabbit.logger = MagicMock()
            rabbit.last_message_consumed_at = 0
            rabbit._reconnect_lock = threading.Lock()
            rabbit.scheduler = MagicMock()
            rabbit.exchange_params = MagicMock()
            rabbit.queue_params = MagicMock()
        return rabbit

    def test_close_connection_when_open(self):
        rabbit = self._make_rabbit()
        mock_conn = MagicMock()
        mock_conn.is_closed = False
        rabbit.connection = mock_conn

        rabbit._close_connection()

        mock_conn.close.assert_called_once()
        self.assertIsNone(rabbit.connection)
        self.assertIsNone(rabbit.channel)

    def test_close_connection_when_already_closed(self):
        rabbit = self._make_rabbit()
        mock_conn = MagicMock()
        mock_conn.is_closed = True
        rabbit.connection = mock_conn

        rabbit._close_connection()

        mock_conn.close.assert_not_called()
        self.assertIsNone(rabbit.connection)

    def test_close_connection_when_none(self):
        rabbit = self._make_rabbit()
        rabbit.connection = None

        rabbit._close_connection()

        self.assertIsNone(rabbit.connection)
        self.assertIsNone(rabbit.channel)

    def test_close_connection_handles_close_exception(self):
        rabbit = self._make_rabbit()
        mock_conn = MagicMock()
        mock_conn.is_closed = False
        mock_conn.close.side_effect = Exception("socket error")
        rabbit.connection = mock_conn

        rabbit._close_connection()

        self.assertIsNone(rabbit.connection)
        self.assertIsNone(rabbit.channel)


class TestValidateChannelConnection(TestCase):
    def _make_rabbit(self):
        with patch.dict("os.environ", {"MQ_URL": "amqp://localhost", "MQ_EXCHANGE": "test"}):
            from amqpstorm_flask.RabbitMQ import RabbitMQ
            rabbit = RabbitMQ.__new__(RabbitMQ)
            rabbit.connection = None
            rabbit.channel = None
            rabbit.mq_url = "amqp://localhost"
            rabbit.mq_exchange = "test"
            rabbit.logger = MagicMock()
            rabbit.last_message_consumed_at = 0
            rabbit._reconnect_lock = threading.Lock()
            rabbit.scheduler = MagicMock()
            rabbit.exchange_params = MagicMock()
            rabbit.queue_params = MagicMock()
        return rabbit

    @patch("amqpstorm_flask.RabbitMQ.UriConnection")
    def test_validate_handles_none_channel(self, mock_uri_conn):
        rabbit = self._make_rabbit()
        rabbit.connection = None
        rabbit.channel = None

        mock_conn = MagicMock()
        mock_conn.is_closed = False
        mock_channel = MagicMock()
        mock_channel.is_closed = False
        mock_conn.channel.return_value = mock_channel
        mock_uri_conn.return_value = mock_conn

        rabbit._validate_channel_connection()

        mock_uri_conn.assert_called_once_with("amqp://localhost")
        self.assertEqual(rabbit.connection, mock_conn)
        self.assertEqual(rabbit.channel, mock_channel)

    @patch("amqpstorm_flask.RabbitMQ.UriConnection")
    def test_validate_closes_old_connection(self, mock_uri_conn):
        rabbit = self._make_rabbit()

        old_conn = MagicMock()
        old_conn.is_closed = False
        rabbit.connection = old_conn
        rabbit.channel = MagicMock()
        rabbit.channel.is_closed = False

        rabbit.last_message_consumed_at = -1

        new_conn = MagicMock()
        new_conn.is_closed = False
        new_channel = MagicMock()
        new_channel.is_closed = False
        new_conn.channel.return_value = new_channel
        mock_uri_conn.return_value = new_conn

        rabbit._validate_channel_connection()

        old_conn.close.assert_called_once()
        self.assertEqual(rabbit.connection, new_conn)
        self.assertEqual(rabbit.channel, new_channel)

    @patch("amqpstorm_flask.RabbitMQ.UriConnection")
    def test_validate_skips_when_healthy(self, mock_uri_conn):
        rabbit = self._make_rabbit()

        mock_conn = MagicMock()
        mock_conn.is_closed = False
        mock_channel = MagicMock()
        mock_channel.is_closed = False
        rabbit.connection = mock_conn
        rabbit.channel = mock_channel
        rabbit.last_message_consumed_at = 0

        rabbit._validate_channel_connection()

        mock_uri_conn.assert_not_called()

    @patch("amqpstorm_flask.RabbitMQ.UriConnection")
    def test_validate_lock_prevents_concurrent_reconnection(self, mock_uri_conn):
        rabbit = self._make_rabbit()
        rabbit.connection = None
        rabbit.channel = None

        call_count = 0
        barrier = threading.Barrier(2, timeout=5)

        original_uri = mock_uri_conn

        def slow_connect(url):
            nonlocal call_count
            call_count += 1
            barrier.wait()
            time.sleep(0.1)
            conn = MagicMock()
            conn.is_closed = False
            channel = MagicMock()
            channel.is_closed = False
            conn.channel.return_value = channel
            return conn

        mock_uri_conn.side_effect = slow_connect

        t1 = threading.Thread(target=rabbit._validate_channel_connection)
        t2 = threading.Thread(target=rabbit._validate_channel_connection)
        t1.start()
        t2.start()
        t1.join(timeout=5)
        t2.join(timeout=5)

        self.assertEqual(call_count, 1)

    @patch("amqpstorm_flask.RabbitMQ.UriConnection")
    def test_validate_logs_error_on_connection_failure(self, mock_uri_conn):
        rabbit = self._make_rabbit()
        rabbit.connection = None
        rabbit.channel = None

        mock_uri_conn.side_effect = Exception("Connection refused")

        rabbit._validate_channel_connection()

        rabbit.logger.error.assert_called_once()
        self.assertIn("Connection refused", rabbit.logger.error.call_args[0][0])


class TestNewConsumerNoRecursion(TestCase):
    @patch("amqpstorm_flask.RabbitMQ.sleep")
    @patch("amqpstorm_flask.RabbitMQ.UriConnection")
    @patch.dict("os.environ", {
        "AMQP_STORM_APSCHEDULER": "0",
        "WERKZEUG_RUN_MAIN": "true",
        "MQ_URL": "amqp://localhost",
        "MQ_EXCHANGE": "test",
    })
    def test_consumer_retries_without_stack_overflow(self, mock_uri_conn, mock_sleep):
        from amqpstorm_flask.RabbitMQ import RabbitMQ

        attempt = 0
        max_failures = 5
        done_event = threading.Event()

        def side_effect(url):
            nonlocal attempt
            attempt += 1
            if attempt <= max_failures:
                raise Exception(f"Failure {attempt}")
            conn = MagicMock()
            conn.is_closed = False
            channel = MagicMock()
            channel.is_closed = False

            def stop_consuming():
                done_event.set()
                raise Exception("done")
            channel.start_consuming.side_effect = stop_consuming
            conn.channel.return_value = channel
            return conn

        mock_uri_conn.side_effect = side_effect

        rabbit = RabbitMQ.__new__(RabbitMQ)
        rabbit.connection = None
        rabbit.channel = None
        rabbit.mq_url = "amqp://localhost"
        rabbit.mq_exchange = "test"
        rabbit.logger = MagicMock()
        rabbit.last_message_consumed_at = 0
        rabbit._reconnect_lock = threading.Lock()
        rabbit.scheduler = MagicMock()
        rabbit.exchange_params = MagicMock()
        rabbit.queue_params = MagicMock()
        rabbit.queue_params.no_ack = True
        rabbit.queue_params.durable = True
        rabbit.queue_params.passive = False
        rabbit.queue_params.auto_delete = False
        rabbit.json_encoder = None
        rabbit.development = False

        @rabbit.queue(routing_key="test.key", queue_name="test-queue")
        def test_handler(routing_key, body, message_id):
            pass

        done_event.wait(timeout=10)
        self.assertTrue(done_event.is_set())
        self.assertGreater(attempt, max_failures)


class TestStop(TestCase):
    def test_stop_closes_connection(self):
        with patch.dict("os.environ", {"MQ_URL": "amqp://localhost", "MQ_EXCHANGE": "test"}):
            from amqpstorm_flask.RabbitMQ import RabbitMQ
            rabbit = RabbitMQ.__new__(RabbitMQ)
            rabbit.connection = MagicMock()
            rabbit.connection.is_closed = False
            rabbit.channel = MagicMock()
            rabbit.logger = MagicMock()
            rabbit._reconnect_lock = threading.Lock()
            rabbit.scheduler = MagicMock()

            rabbit.stop()

            rabbit.connection_ref = rabbit.connection
            rabbit.scheduler.shutdown.assert_called_once()

    def test_stop_with_no_connection(self):
        with patch.dict("os.environ", {"MQ_URL": "amqp://localhost", "MQ_EXCHANGE": "test"}):
            from amqpstorm_flask.RabbitMQ import RabbitMQ
            rabbit = RabbitMQ.__new__(RabbitMQ)
            rabbit.connection = None
            rabbit.channel = None
            rabbit.logger = MagicMock()
            rabbit._reconnect_lock = threading.Lock()
            rabbit.scheduler = MagicMock()

            rabbit.stop()

            rabbit.scheduler.shutdown.assert_called_once()
            self.assertIsNone(rabbit.connection)
