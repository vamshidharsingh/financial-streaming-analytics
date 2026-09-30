import json
import time
import uuid
import random
from datetime import datetime
from confluent_kafka import Producer
from faker import Faker

fake = Faker()

KAFKA_TOPIC = 'financial_transactions'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def generate_transaction():
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": str(uuid.uuid4()),
        "amount": round(random.uniform(10.0, 5000.0), 2),
        "currency": random.choice(["USD", "EUR", "GBP", "JPY"]),
        "transaction_type": random.choice(["deposit", "withdrawal", "transfer", "payment"]),
        "status": random.choices(["completed", "pending", "failed"], weights=[0.8, 0.15, 0.05])[0],
        "timestamp": datetime.utcnow().isoformat()
    }

def main():
    conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS}
    producer = Producer(conf)

    print(f"Starting producer. Sending to topic '{KAFKA_TOPIC}'...")
    try:
        while True:
            transaction = generate_transaction()
            producer.produce(
                KAFKA_TOPIC,
                key=transaction['user_id'].encode('utf-8'),
                value=json.dumps(transaction).encode('utf-8'),
                callback=delivery_report
            )
            producer.poll(0)
            time.sleep(random.uniform(0.1, 2.0))
    except KeyboardInterrupt:
        print("Producer stopped.")
    finally:
        producer.flush()

if __name__ == "__main__":
    main()
