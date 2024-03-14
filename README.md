
# AMQP Service for Flask Apps



## Features

- Connection validation and reconnection for AMQP.
- Sending messages to a specified exchange with retries.
- Consuming messages from a queue with retries.
- Customizable parameters for exchanges and queues.
- Message headers and properties support.
- Multi-threading support for consumers.

## Prerequisites

- Python 3.x
- Flask
- amqpstorm
- retry

## Installation

To install the required dependencies, run:

```bash
pip install amqpstorm-flask
```

## Usage

### Initialization

First, create a Flask app and initialize the `RabbitMQ`.

```python
from flask import Flask
app = Flask(__name__)
app.config["MQ_URL"] = "<Your_MQ_URL>"
app.config["MQ_EXCHANGE"] = "<Your_MQ_Exchange_Name>"

amqp_service = RabbitMQ()
amqp_service.init_app(app)
```

### Sending Messages

To send a message to a specified exchange:

```python
amqp_service.send(
    body={"key": "value"},
    routing_key="route.key",
    exchange_type="direct",
    retries=5,
)
```

### Consuming Messages

To consume messages from a specified queue:

```python
@amqp_service.queue(
    queue_name="test_queue",
    routing_key="route.key",
)
def process_message(message):
    print(message.body)
```

