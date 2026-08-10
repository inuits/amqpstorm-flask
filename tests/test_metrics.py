import calendar
import time
from unittest import TestCase
from unittest.mock import patch

from prometheus_client import REGISTRY

from amqpstorm_flask.metrics import (
    PUBLISHED_AT_HEADER,
    instrument_consumer,
    published_at,
)
from tests.test_rabbitmq import _make_mock_connection, _make_rabbit

LABELS = {"env": "env", "service_name": "nomad_group_name", "queue": "test.queue"}


class FakeMessage:
    """Stand-in for amqpstorm's Message, which forbids attribute injection."""

    __slots__ = ["properties", "body", "settlements"]

    def __init__(self, properties=None, body="{}"):
        self.properties = properties if properties is not None else {}
        self.body = body
        self.settlements = []

    def ack(self):
        self.settlements.append(("ack", None))
        return "acked"

    def nack(self, requeue=True):
        self.settlements.append(("nack", requeue))
        return "nacked"

    def reject(self, requeue=True):
        self.settlements.append(("reject", requeue))
        return "rejected"

    def json(self):
        return {"parsed": True}


def _published_message(seconds_ago=0.0):
    return FakeMessage(
        properties={"headers": {PUBLISHED_AT_HEADER: time.time() - seconds_ago}}
    )


def _count(metric):
    return REGISTRY.get_sample_value(f"{metric}_count", LABELS) or 0


def _sum(metric):
    return REGISTRY.get_sample_value(f"{metric}_sum", LABELS) or 0


class MetricAssertions(TestCase):
    def setUp(self):
        self.before = {
            metric: _count(metric)
            for metric in (
                "message_processing_active_duration_seconds",
                "message_processing_waiting_duration_seconds",
                "message_processing_total_duration_seconds",
            )
        }

    def assertObserved(self, metric, times=1):
        self.assertEqual(_count(metric) - self.before[metric], times)

    def assertNotObserved(self, metric):
        self.assertObserved(metric, times=0)


class TestPublishedAt(TestCase):
    def test_reads_header(self):
        message = FakeMessage(properties={"headers": {PUBLISHED_AT_HEADER: 1700000000.5}})

        self.assertEqual(published_at(message), 1700000000.5)

    def test_falls_back_to_amqp_timestamp_struct_time(self):
        # pamqp decodes the AMQP timestamp property with time.gmtime().
        epoch = 1700000000
        message = FakeMessage(properties={"timestamp": time.gmtime(epoch)})

        self.assertEqual(published_at(message), float(epoch))

    def test_header_wins_over_timestamp(self):
        message = FakeMessage(
            properties={
                "headers": {PUBLISHED_AT_HEADER: 1700000000.5},
                "timestamp": time.gmtime(1600000000),
            }
        )

        self.assertEqual(published_at(message), 1700000000.5)

    def test_returns_none_without_publication_time(self):
        self.assertIsNone(published_at(FakeMessage()))

    def test_ignores_unparsable_header(self):
        message = FakeMessage(properties={"headers": {PUBLISHED_AT_HEADER: "nope"}})

        self.assertIsNone(published_at(message))


class TestInstrumentedConsumer(MetricAssertions):
    def test_active_and_waiting_are_observed(self):
        message = _published_message(seconds_ago=2)

        instrument_consumer(lambda msg: msg.ack(), queue="test.queue", auto_ack=False)(message)

        self.assertObserved("message_processing_active_duration_seconds")
        self.assertObserved("message_processing_waiting_duration_seconds")
        self.assertGreaterEqual(
            _sum("message_processing_waiting_duration_seconds"), 2
        )

    def test_total_is_observed_on_ack(self):
        message = _published_message(seconds_ago=1)

        instrument_consumer(lambda msg: msg.ack(), queue="test.queue", auto_ack=False)(message)

        self.assertObserved("message_processing_total_duration_seconds")
        self.assertEqual(message.settlements, [("ack", None)])

    def test_no_total_when_nacked_with_requeue(self):
        message = _published_message()

        instrument_consumer(lambda msg: msg.nack(), queue="test.queue", auto_ack=False)(message)

        self.assertNotObserved("message_processing_total_duration_seconds")
        self.assertObserved("message_processing_active_duration_seconds")
        self.assertEqual(message.settlements, [("nack", True)])

    def test_total_is_observed_when_nacked_without_requeue(self):
        message = _published_message()

        instrument_consumer(
            lambda msg: msg.nack(requeue=False), queue="test.queue", auto_ack=False
        )(message)

        self.assertObserved("message_processing_total_duration_seconds")

    def test_total_is_observed_when_rejected_without_requeue(self):
        message = _published_message()

        instrument_consumer(
            lambda msg: msg.reject(requeue=False), queue="test.queue", auto_ack=False
        )(message)

        self.assertObserved("message_processing_total_duration_seconds")

    def test_no_total_when_rejected_with_requeue(self):
        message = _published_message()

        instrument_consumer(lambda msg: msg.reject(), queue="test.queue", auto_ack=False)(message)

        self.assertNotObserved("message_processing_total_duration_seconds")

    def test_no_total_when_message_is_never_settled(self):
        message = _published_message()

        instrument_consumer(lambda msg: None, queue="test.queue", auto_ack=False)(message)

        self.assertNotObserved("message_processing_total_duration_seconds")

    def test_total_is_observed_at_callback_end_when_auto_acking(self):
        message = _published_message()

        instrument_consumer(lambda msg: None, queue="test.queue", auto_ack=True)(message)

        self.assertObserved("message_processing_total_duration_seconds")

    def test_total_is_observed_once_per_message(self):
        def consume(msg):
            msg.ack()
            msg.ack()

        instrument_consumer(consume, queue="test.queue", auto_ack=True)(_published_message())

        self.assertObserved("message_processing_total_duration_seconds")

    def test_durations_are_observed_when_callback_raises(self):
        def consume(msg):
            raise ValueError("boom")

        with self.assertRaises(ValueError):
            instrument_consumer(consume, queue="test.queue", auto_ack=False)(
                _published_message()
            )

        self.assertObserved("message_processing_active_duration_seconds")
        self.assertObserved("message_processing_waiting_duration_seconds")
        self.assertNotObserved("message_processing_total_duration_seconds")

    def test_waiting_and_total_are_skipped_without_publication_time(self):
        instrument_consumer(lambda msg: msg.ack(), queue="test.queue", auto_ack=False)(
            FakeMessage()
        )

        self.assertObserved("message_processing_active_duration_seconds")
        self.assertNotObserved("message_processing_waiting_duration_seconds")
        self.assertNotObserved("message_processing_total_duration_seconds")

    def test_callback_return_value_is_passed_through(self):
        result = instrument_consumer(
            lambda msg: msg.ack(), queue="test.queue", auto_ack=False
        )(_published_message())

        self.assertEqual(result, "acked")

    def test_message_attributes_are_proxied(self):
        seen = {}

        def consume(msg):
            seen["body"] = msg.body
            seen["json"] = msg.json()
            seen["headers"] = msg.properties["headers"]

        message = _published_message()
        instrument_consumer(consume, queue="test.queue", auto_ack=True)(message)

        self.assertEqual(seen["body"], "{}")
        self.assertEqual(seen["json"], {"parsed": True})
        self.assertIn(PUBLISHED_AT_HEADER, seen["headers"])


class TestQueueDecoratorWiring(TestCase):
    """The queue() decorator must hand an instrumented callback to basic.consume."""

    @patch("amqpstorm_flask.RabbitMQ.UriConnection")
    def test_consume_callback_is_instrumented(self, mock_uri_conn):
        rabbit = _make_rabbit()
        conn = _make_mock_connection()
        mock_uri_conn.return_value = conn
        rabbit.connection = conn
        rabbit.channel = conn.channel.return_value
        rabbit.queue_params.no_ack = False
        consumer_channel = conn.channel.return_value
        # Break out of new_consumer() instead of blocking on the consume loop.
        consumer_channel.start_consuming.side_effect = RuntimeError("stop consuming")

        @rabbit.queue(
            routing_key="test.key",
            queue_name="wiring.queue",
            auto_ack=False,
            full_message_object=True,
        )
        def handler(message):
            return message.ack()

        rabbit.scheduler.add_job.call_args.args[0]()

        callback = consumer_channel.basic.consume.call_args.args[0]
        self.assertIsNot(callback, handler)
        self.assertIs(consumer_channel.basic.consume.call_args.kwargs["no_ack"], False)

        labels = dict(LABELS, queue="wiring.queue")
        message = FakeMessage(
            properties={"headers": {PUBLISHED_AT_HEADER: time.time() - 1}}
        )
        callback(message)

        self.assertEqual(message.settlements, [("ack", None)])
        for metric in (
            "message_processing_active_duration_seconds",
            "message_processing_waiting_duration_seconds",
            "message_processing_total_duration_seconds",
        ):
            self.assertEqual(
                REGISTRY.get_sample_value(f"{metric}_count", labels), 1, metric
            )


class TestPublishedAtRoundTrip(TestCase):
    def test_amqp_timestamp_round_trip_matches_epoch(self):
        # RabbitMQ.py publishes properties["timestamp"] as int epoch seconds;
        # pamqp encodes with calendar.timegm() and decodes with time.gmtime().
        epoch = int(time.time())

        decoded = time.gmtime(epoch)

        self.assertEqual(calendar.timegm(decoded), epoch)
