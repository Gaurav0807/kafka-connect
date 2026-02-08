import time
import uuid
import random
from faker import Faker
from confluent_kafka.avro import AvroProducer, loads

fake = Faker()

KAFKA_BOOTSTRAP = "kafka1:29092"
SCHEMA_REGISTRY_URL = "http://schema-registry:8081"
TOPIC = "user_events"

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

def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Produced to {msg.topic()} [{msg.partition()}] offset {msg.offset()}")

print("🚀 Producing events to Kafka...")

while True:
    event = {
        "event_id": str(uuid.uuid4()),
        "user_id": str(random.randint(1000, 9999)),
        "event_type": random.choice(["click", "view", "purchase"]),
        "device": random.choice(["android", "ios", "web"]),
        "country": fake.country_code(),
        "event_time": int(time.time() * 1000)
    }

    producer.produce(
        topic=TOPIC,
        value=event,
        on_delivery=delivery_report
    )

    producer.flush()
    #time.sleep(1)
