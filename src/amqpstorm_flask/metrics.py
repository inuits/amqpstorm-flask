import calendar
from datetime import datetime
from os import getenv
from time import struct_time, time

from prometheus_client import Histogram
from prometheus_client.utils import INF

# Header carrying the publication time as epoch seconds (float). The AMQP
# `timestamp` property only has one second resolution, and pamqp encodes it
# through time.timegm(), so a sub second header is both more precise and
# immune to the local/UTC confusion in that codec. Messages published by
# older library versions or other producers fall back to `timestamp`.
PUBLISHED_AT_HEADER = "x-published-at"

METRICS_ENABLED = int(getenv("AMQP_METRICS_ENABLED", 1)) == 1

_ACTIVE_BUCKETS = (.005, .01, .025, .05, .075, .1, .25, .5, .75, 1.0, 2.5, 5.0, 7.5, 10.0, INF)
# Waiting and total include the time spent sitting in the queue, which is
# measured in seconds to minutes rather than milliseconds.
_QUEUED_BUCKETS = (.01, .05, .1, .25, .5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0, INF)

_LABELS = ["env", "service_name", "queue"]


def _buckets(env_var, default):
    configured = getenv(env_var, "")
    if not configured:
        return default
    return tuple(float(bucket) for bucket in configured.split(","))


message_processing_active_duration = Histogram(
    "message_processing_active_duration_seconds",
    "Duration of the consumer callback for a message in seconds",
    _LABELS,
    buckets=_buckets("MESSAGE_PROCESSING_ACTIVE_DURATION_BUCKETS", _ACTIVE_BUCKETS),
)

message_processing_waiting_duration = Histogram(
    "message_processing_waiting_duration_seconds",
    "Time between publishing a message and the start of its processing in seconds",
    _LABELS,
    buckets=_buckets("MESSAGE_PROCESSING_WAITING_DURATION_BUCKETS", _QUEUED_BUCKETS),
)

message_processing_total_duration = Histogram(
    "message_processing_total_duration_seconds",
    "Time between publishing a message and acknowledging it in seconds",
    _LABELS,
    buckets=_buckets("MESSAGE_PROCESSING_TOTAL_DURATION_BUCKETS", _QUEUED_BUCKETS),
)


def published_at(message):
    """Epoch seconds at which the message was published, or None if unknown."""
    properties = getattr(message, "properties", None) or {}
    raw = (properties.get("headers") or {}).get(PUBLISHED_AT_HEADER)
    if raw is not None:
        try:
            return float(raw)
        except (TypeError, ValueError):
            pass

    timestamp = properties.get("timestamp")
    if isinstance(timestamp, struct_time):
        # pamqp decodes the AMQP timestamp with time.gmtime().
        return float(calendar.timegm(timestamp))
    if isinstance(timestamp, datetime):
        if timestamp.tzinfo is not None:
            return timestamp.timestamp()
        return float(calendar.timegm(timestamp.timetuple()))
    if isinstance(timestamp, (int, float)):
        return float(timestamp)
    return None


class InstrumentedMessage:
    """Transparent proxy around an amqpstorm Message.

    Observes message_processing_total_duration_seconds once the message reaches
    a terminal state. A nack or reject that requeues the message is not
    terminal: the message will be delivered again, so counting it would report
    a total for work that is not finished yet. amqpstorm's Message uses
    __slots__, so the ack/nack hooks cannot be attached to the instance itself.
    """

    def __init__(self, message, labels, published_at_seconds):
        object.__setattr__(self, "_message", message)
        object.__setattr__(self, "_labels", labels)
        object.__setattr__(self, "_published_at", published_at_seconds)
        object.__setattr__(self, "_settled", False)

    @property
    def settled(self):
        return object.__getattribute__(self, "_settled")

    def observe_total(self):
        if self.settled:
            return
        object.__setattr__(self, "_settled", True)
        published = object.__getattribute__(self, "_published_at")
        if published is None:
            return
        message_processing_total_duration.labels(
            **object.__getattribute__(self, "_labels")
        ).observe(max(0.0, time() - published))

    def ack(self):
        result = object.__getattribute__(self, "_message").ack()
        self.observe_total()
        return result

    def nack(self, requeue=True):
        result = object.__getattribute__(self, "_message").nack(requeue=requeue)
        if not requeue:
            self.observe_total()
        return result

    def reject(self, requeue=True):
        result = object.__getattribute__(self, "_message").reject(requeue=requeue)
        if not requeue:
            self.observe_total()
        return result

    def __getattr__(self, item):
        return getattr(object.__getattribute__(self, "_message"), item)

    def __setattr__(self, key, value):
        setattr(object.__getattribute__(self, "_message"), key, value)


def instrument_consumer(callback, queue, auto_ack):
    """Wrap a consumer callback with the message_processing_* metrics.

    `auto_ack` consumers never settle the message themselves, so the end of the
    callback is the only terminal moment available for the total duration.
    """
    if not METRICS_ENABLED:
        return callback

    labels = {
        "env": getenv("NOMAD_JOB_NAME", "nomad_job_name-env").split("-")[-1],
        "service_name": getenv("NOMAD_GROUP_NAME", "nomad_group_name"),
        "queue": queue,
    }

    def instrumented_callback(message):
        published = published_at(message)
        started_at = time()
        if published is not None:
            message_processing_waiting_duration.labels(**labels).observe(
                max(0.0, started_at - published)
            )
        instrumented_message = InstrumentedMessage(message, labels, published)
        try:
            return callback(instrumented_message)
        finally:
            message_processing_active_duration.labels(**labels).observe(
                time() - started_at
            )
            if auto_ack:
                instrumented_message.observe_total()

    return instrumented_callback
