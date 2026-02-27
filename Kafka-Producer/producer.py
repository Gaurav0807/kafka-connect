import time
import uuid
import random
from faker import Faker
from confluent_kafka.avro import AvroProducer, loads
from prometheus_client import Counter, Histogram, start_http_server

fake = Faker()

KAFKA_BOOTSTRAP = "kafka1:29092"
SCHEMA_REGISTRY_URL = "http://schema-registry:8081"
TOPIC = "user_events"

MESSAGES_PRODUCED = Counter(
    'kafka_producer_messages_total',
    'Total messages produced',
    ['topic', 'status']
)
DELIVERY_LATENCY = Histogram(
    'kafka_producer_delivery_latency_seconds',
    'Message delivery latency in seconds',
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]
)

value_schema_str = """
{
  "type": "record",
  "name": "UserEvent",
  "namespace": "com.demo.events",
  "fields": [
    { "name": "event_id", "type": "string" },
    { "name": "user_id", "type": "string" },
    { "name": "event_type", "type": "string" },
    { "name": "device", "type": "string" },
    { "name": "country", "type": "string" },
    { "name": "event_time", "type": "long" }
  ]
}
"""

value_schema = loads(value_schema_str)

producer = AvroProducer(
    {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "schema.registry.url": SCHEMA_REGISTRY_URL
    },
    default_value_schema=value_schema
)

_produce_time = None

def delivery_report(err, msg):
    global _produce_time
    if err:
        print(f"Delivery failed: {err}")
        MESSAGES_PRODUCED.labels(topic=TOPIC, status='error').inc()
    else:
        print(f"Produced to {msg.topic()} [{msg.partition()}] offset {msg.offset()}")
        MESSAGES_PRODUCED.labels(topic=TOPIC, status='success').inc()
        if _produce_time is not None:
            DELIVERY_LATENCY.observe(time.time() - _produce_time)

# Start Prometheus metrics HTTP server
start_http_server(8000)
print("Prometheus metrics server started on port 8000")
print("Producing events to Kafka...")

while True:
    event = {
        "event_id": str(uuid.uuid4()),
        "user_id": str(random.randint(1000, 9999)),
        "event_type": random.choice(["click", "view", "purchase"]),
        "device": random.choice(["android", "ios", "web"]),
        "country": fake.country_code(),
        "event_time": int(time.time() * 1000)
    }

    _produce_time = time.time()
    producer.produce(
        topic=TOPIC,
        value=event,
        on_delivery=delivery_report
    )

    producer.flush()
    #time.sleep(1)
