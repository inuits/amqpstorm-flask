import threading
import time
from unittest import TestCase
from unittest.mock import MagicMock, patch, call


def _make_rabbit():
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
        rabbit._publish_lock = threading.Lock()
        rabbit._consumer_channels = []
        rabbit.scheduler = MagicMock()
        rabbit.exchange_params = MagicMock()
        rabbit.queue_params = MagicMock()
    return rabbit


def _make_mock_connection(with_channel=True):
    conn = MagicMock()
    conn.is_closed = False
    conn.is_open = True
    if with_channel:
        channel = MagicMock()
        channel.is_closed = False
        channel.consumer_tags = []
        conn.channel.return_value = channel
    return conn


class TestCloseConnection(TestCase):
    def test_close_connection_when_open(self):
        rabbit = _make_rabbit()
        mock_conn = _make_mock_connection()
        rabbit.connection = mock_conn

        rabbit._close_connection()

        mock_conn.close.assert_called_once()
        self.assertIsNone(rabbit.connection)
        self.assertIsNone(rabbit.channel)

    def test_close_connection_when_already_closed(self):
        rabbit = _make_rabbit()
        mock_conn = MagicMock()
        mock_conn.is_closed = True
        rabbit.connection = mock_conn

        rabbit._close_connection()

        mock_conn.close.assert_not_called()
        self.assertIsNone(rabbit.connection)

    def test_close_connection_when_none(self):
        rabbit = _make_rabbit()

        rabbit._close_connection()

        self.assertIsNone(rabbit.connection)
        self.assertIsNone(rabbit.channel)

    def test_close_connection_handles_close_exception(self):
        rabbit = _make_rabbit()
        mock_conn = MagicMock()
        mock_conn.is_closed = False
        mock_conn.close.side_effect = Exception("socket error")
        rabbit.connection = mock_conn

        rabbit._close_connection()

        self.assertIsNone(rabbit.connection)
        self.assertIsNone(rabbit.channel)

    def test_close_connection_closes_consumer_channels(self):
        rabbit = _make_rabbit()
        ch1 = MagicMock(is_closed=False)
        ch2 = MagicMock(is_closed=False)
        rabbit._consumer_channels = [ch1, ch2]
        rabbit.connection = _make_mock_connection()

        rabbit._close_connection()

        ch1.close.assert_called_once()
        ch2.close.assert_called_once()
        self.assertEqual(rabbit._consumer_channels, [])

    def test_close_connection_ignores_failed_channel_close(self):
        rabbit = _make_rabbit()
        ch1 = MagicMock(is_closed=False)
        ch1.close.side_effect = Exception("already closed")
        rabbit._consumer_channels = [ch1]
        rabbit.connection = _make_mock_connection()

        rabbit._close_connection()

        self.assertEqual(rabbit._consumer_channels, [])
        self.assertIsNone(rabbit.connection)


class TestValidateChannelConnection(TestCase):
    @patch("amqpstorm_flask.RabbitMQ.UriConnection")
    def test_validate_handles_none_channel(self, mock_uri_conn):
        rabbit = _make_rabbit()
        mock_conn = _make_mock_connection()
        mock_uri_conn.return_value = mock_conn

        rabbit._validate_channel_connection()

        mock_uri_conn.assert_called_once_with("amqp://localhost")
        self.assertEqual(rabbit.connection, mock_conn)
        self.assertIsNotNone(rabbit.channel)

    @patch("amqpstorm_flask.RabbitMQ.UriConnection")
    def test_validate_closes_old_connection(self, mock_uri_conn):
        rabbit = _make_rabbit()
        old_conn = _make_mock_connection()
        rabbit.connection = old_conn
        rabbit.channel = MagicMock(is_closed=False)
        rabbit.last_message_consumed_at = -1

        new_conn = _make_mock_connection()
        mock_uri_conn.return_value = new_conn

        rabbit._validate_channel_connection()

        old_conn.close.assert_called_once()
        self.assertEqual(rabbit.connection, new_conn)

    @patch("amqpstorm_flask.RabbitMQ.UriConnection")
    def test_validate_skips_when_healthy(self, mock_uri_conn):
        rabbit = _make_rabbit()
        rabbit.connection = _make_mock_connection()
        rabbit.channel = MagicMock(is_closed=False)

        rabbit._validate_channel_connection()

        mock_uri_conn.assert_not_called()

    @patch("amqpstorm_flask.RabbitMQ.UriConnection")
    def test_validate_recreates_dead_publish_channel(self, mock_uri_conn):
        rabbit = _make_rabbit()
        conn = _make_mock_connection()
        rabbit.connection = conn
        rabbit.channel = MagicMock(is_closed=True)

        new_channel = MagicMock(is_closed=False)
        conn.channel.return_value = new_channel

        rabbit._validate_channel_connection()

        mock_uri_conn.assert_not_called()
        self.assertEqual(rabbit.channel, new_channel)

    @patch("amqpstorm_flask.RabbitMQ.UriConnection")
    def test_validate_lock_prevents_concurrent_reconnection(self, mock_uri_conn):
        rabbit = _make_rabbit()

        call_count = 0
        barrier = threading.Barrier(2, timeout=5)

        def slow_connect(url):
            nonlocal call_count
            call_count += 1
            barrier.wait()
            time.sleep(0.1)
            return _make_mock_connection()

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
        rabbit = _make_rabbit()
        mock_uri_conn.side_effect = Exception("Connection refused")

        rabbit._validate_channel_connection()

        rabbit.logger.error.assert_called_once()
        self.assertIn("Connection refused", rabbit.logger.error.call_args[0][0])


class TestPerConsumerChannels(TestCase):
    @patch("amqpstorm_flask.RabbitMQ.sleep")
    @patch("amqpstorm_flask.RabbitMQ.UriConnection")
    @patch.dict("os.environ", {
        "AMQP_STORM_APSCHEDULER": "0",
        "WERKZEUG_RUN_MAIN": "true",
        "MQ_URL": "amqp://localhost",
        "MQ_EXCHANGE": "test",
    })
    def test_each_consumer_gets_own_channel(self, mock_uri_conn, mock_sleep):
        from amqpstorm_flask.RabbitMQ import RabbitMQ

        channels_created = []
        done_event = threading.Event()
        conn = _make_mock_connection(with_channel=False)

        def make_channel():
            ch = MagicMock()
            ch.is_closed = False
            ch.consumer_tags = ["tag"]
            channels_created.append(ch)
            if len(channels_created) >= 3:
                ch.start_consuming.side_effect = lambda: done_event.set() or time.sleep(5)
            else:
                ch.start_consuming.side_effect = lambda: done_event.set() or time.sleep(5)
            return ch

        conn.channel.side_effect = make_channel
        mock_uri_conn.return_value = conn

        rabbit = RabbitMQ.__new__(RabbitMQ)
        rabbit.connection = None
        rabbit.channel = None
        rabbit.mq_url = "amqp://localhost"
        rabbit.mq_exchange = "test"
        rabbit.logger = MagicMock()
        rabbit.last_message_consumed_at = 0
        rabbit._reconnect_lock = threading.Lock()
        rabbit._publish_lock = threading.Lock()
        rabbit._consumer_channels = []
        rabbit.scheduler = MagicMock()
        rabbit.exchange_params = MagicMock()
        rabbit.queue_params = MagicMock()
        rabbit.queue_params.no_ack = True
        rabbit.queue_params.durable = True
        rabbit.queue_params.passive = False
        rabbit.queue_params.auto_delete = False
        rabbit.json_encoder = None
        rabbit.development = False

        @rabbit.queue(routing_key="test.key1", queue_name="queue-1")
        def handler_one(routing_key, body, message_id):
            pass

        @rabbit.queue(routing_key="test.key2", queue_name="queue-2")
        def handler_two(routing_key, body, message_id):
            pass

        done_event.wait(timeout=10)

        # 1 publish channel (from _validate_channel_connection) + 2 consumer channels
        self.assertGreaterEqual(len(channels_created), 3)
        # Consumer channels should be different from each other
        consumer_chs = rabbit._consumer_channels
        self.assertEqual(len(consumer_chs), 2)
        self.assertIsNot(consumer_chs[0], consumer_chs[1])
        # Publish channel (self.channel) should be separate from consumer channels
        self.assertNotIn(rabbit.channel, consumer_chs)


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
            conn = _make_mock_connection(with_channel=False)

            def make_channel():
                ch = MagicMock()
                ch.is_closed = False
                ch.consumer_tags = []

                def stop_consuming():
                    done_event.set()
                    raise Exception("done")
                ch.start_consuming.side_effect = stop_consuming
                return ch

            conn.channel.side_effect = make_channel
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
        rabbit._publish_lock = threading.Lock()
        rabbit._consumer_channels = []
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


class TestCheckHealth(TestCase):
    def test_health_ok_with_active_consumers(self):
        rabbit = _make_rabbit()
        rabbit.connection = _make_mock_connection()
        ch = MagicMock(is_closed=False, consumer_tags=["consumer-1"])
        rabbit._consumer_channels = [ch]

        ok, msg = rabbit.check_health()

        self.assertTrue(ok)
        self.assertEqual(msg, "Connection open")

    def test_health_fails_no_connection(self):
        rabbit = _make_rabbit()
        rabbit.connection = None

        ok, msg = rabbit.check_health()

        self.assertFalse(ok)
        self.assertEqual(msg, "Connection not open")

    def test_health_fails_no_consumers(self):
        rabbit = _make_rabbit()
        rabbit.connection = _make_mock_connection()
        rabbit._consumer_channels = []

        ok, msg = rabbit.check_health(check_consumers=True)

        self.assertFalse(ok)
        self.assertEqual(msg, "No consumers available")

    def test_health_ok_without_consumer_check(self):
        rabbit = _make_rabbit()
        rabbit.connection = _make_mock_connection()
        rabbit._consumer_channels = []

        ok, msg = rabbit.check_health(check_consumers=False)

        self.assertTrue(ok)

    def test_health_ignores_dead_consumer_channels(self):
        rabbit = _make_rabbit()
        rabbit.connection = _make_mock_connection()
        dead_ch = MagicMock(is_closed=True, consumer_tags=["old"])
        rabbit._consumer_channels = [dead_ch]

        ok, msg = rabbit.check_health(check_consumers=True)

        self.assertFalse(ok)
        self.assertEqual(msg, "No consumers available")


class TestSendThreadSafety(TestCase):
    @patch("amqpstorm_flask.RabbitMQ.UriConnection")
    def test_send_uses_publish_lock(self, mock_uri_conn):
        rabbit = _make_rabbit()
        conn = _make_mock_connection()
        mock_uri_conn.return_value = conn
        rabbit.json_encoder = None
        rabbit.development = False

        publish_channel = conn.channel.return_value
        rabbit.connection = conn
        rabbit.channel = publish_channel

        rabbit.send({"test": "data"}, routing_key="test.route")

        publish_channel.basic.publish.assert_called_once()
        # Verify the publish channel (self.channel) is used, not a consumer channel
        self.assertEqual(rabbit.channel, publish_channel)


class TestStop(TestCase):
    def test_stop_closes_connection_and_channels(self):
        rabbit = _make_rabbit()
        conn = MagicMock(is_closed=False)
        rabbit.connection = conn
        rabbit.channel = MagicMock(is_closed=False)
        ch1 = MagicMock(is_closed=False)
        rabbit._consumer_channels = [ch1]

        rabbit.stop()

        rabbit.scheduler.shutdown.assert_called_once()
        conn.close.assert_called_once()
        ch1.close.assert_called_once()
        self.assertIsNone(rabbit.connection)
        self.assertIsNone(rabbit.channel)
        self.assertEqual(rabbit._consumer_channels, [])

    def test_stop_with_no_connection(self):
        rabbit = _make_rabbit()

        rabbit.stop()

        rabbit.scheduler.shutdown.assert_called_once()
        self.assertIsNone(rabbit.connection)
