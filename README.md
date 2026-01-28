# AMQP Service for Flask Apps

A Flask extension for [AMQPStorm](https://github.com/eandersson/amqpstorm) that provides easy integration with RabbitMQ.

## Features

- **Automatic Reconnection**: Uses `APScheduler` to monitor and reconnect to RabbitMQ if the connection drops.
- **Easy Publishing**: Simple method to send JSON messages with retry logic.
- **Decorator-based Consumers**: Define message consumers using simple decorators.
- **Health Checks**: Built-in health check functionality.
- **Configuration**: Configure via Flask app config or environment variables.

## Installation

```bash
pip install amqpstorm-flask
```

## Configuration

The extension can be configured via Flask's `app.config` or environment variables.

| Config Key | Environment Variable | Default | Description |
|------------|----------------------|---------|-------------|
| `MQ_URL` | `MQ_URL` | None | AMQP connection URL (e.g. `amqp://guest:guest@localhost:5672/%2f`) |
| `MQ_EXCHANGE` | `MQ_EXCHANGE` | None | Default exchange name |
| | `AMQP_STORM_APSCHEDULER` | `1` | Use APScheduler for health checks and consumers (recommended) |
| | `FILTER_LOGS` | `1` | Filter out noisy connection logs |
| | `MQ_MAX_CONSUMER_IDLE_TIME` | `300` | Max idle time in seconds before reconnecting |
| | `MQ_DELIMITER` | `.` | Delimiter used to generate queue names from function names |
| | `MQ_QUEUES` | None | Comma-separated list of enabled queues (if set, only these will start) |

## Usage

### Initialization

```python
from flask import Flask
from amqpstorm_flask import RabbitMQ

app = Flask(__name__)
app.config["MQ_URL"] = "amqp://guest:guest@localhost:5672/%2f"
app.config["MQ_EXCHANGE"] = "my_exchange"

mq = RabbitMQ(app)
```

Alternatively, use the factory pattern:

```python
mq = RabbitMQ()
# ... later ...
mq.init_app(app)
```

### Standalone Usage (Without Flask)

If you are not using Flask or want to initialize it manually without the `init_app` helper:

```python
from amqpstorm_flask import RabbitMQ

mq = RabbitMQ(
    mq_url="amqp://guest:guest@localhost:5672/%2f",
    mq_exchange="my_exchange"
)
# Manual start is required if init_app is not used
mq.start()
```

### Sending Messages

```python
mq.send(
    body={"hello": "world"},
    routing_key="events.user.created",
    exchange_type="topic"
)
```

### Consuming Messages

By default, the decorated function receives `routing_key`, `body`, and `message_id`.

```python
@mq.queue(routing_key="events.user.#")
def handle_user_events(routing_key, body, message_id):
    print(f"Received event {routing_key}: {body}")
```

To receive the full `AMQPStorm` message object:

```python
@mq.queue(routing_key="events.user.#", full_message_object=True)
def handle_user_events(message):
    print(f"Received body: {message.body}")
    # Manual acknowledgment if auto_ack is False
    # message.ack()
```

### Health Check

You can use the `check_health` method to implement a health check endpoint:

```python
@app.route("/health")
def health_check():
    is_ok, message = mq.check_health()
    if is_ok:
        return {"status": "ok"}, 200
    return {"status": "error", "message": message}, 503
```

## Advanced Queue Configuration

The `@mq.queue` decorator supports several parameters:

- `routing_key`: String or list of strings.
- `queue_name`: Custom queue name (defaults to function name with `_` replaced by `MQ_DELIMITER`).
- `exchange_type`: Default is `"topic"`.
- `auto_ack`: Whether to automatically acknowledge messages.
- `prefetch_count`: Number of messages to prefetch (default `1`).
- `queue_arguments`: Dictionary of arguments for queue declaration (default `{"x-queue-type": "quorum"}`).
- `full_message_object`: If `True`, the decorated function receives the message object instead of unpacked values.

