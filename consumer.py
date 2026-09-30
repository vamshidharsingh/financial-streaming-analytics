import json
import os
import time
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
import psycopg2
from dotenv import load_dotenv

load_dotenv()

KAFKA_TOPIC = 'financial_transactions'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "admin")
DB_PASSWORD = os.getenv("DB_PASSWORD", "adminpassword")
DB_NAME = os.getenv("DB_NAME", "financial_data")

def get_db_connection():
    while True:
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
            return conn
        except psycopg2.OperationalError as e:
            print(f"Database not ready, retrying in 5 seconds... ({e})")
            time.sleep(5)

def process_message(msg_value):
    try:
        data = json.loads(msg_value)
        # Validation / Transformation
        data['timestamp'] = datetime.fromisoformat(data['timestamp'])
        return data
    except Exception as e:
        print(f"Error processing message: {e}")
        return None

def main():
    conn = get_db_connection()
    cursor = conn.cursor()

    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'financial_consumer_group',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(conf)
    consumer.subscribe([KAFKA_TOPIC])

    print("Starting consumer...")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Consumer error: {msg.error()}")
                    break

            processed_data = process_message(msg.value().decode('utf-8'))
            if processed_data:
                try:
                    cursor.execute("""
                        INSERT INTO transactions (transaction_id, user_id, amount, currency, transaction_type, status, timestamp)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (transaction_id) DO NOTHING
                    """, (
                        processed_data['transaction_id'],
                        processed_data['user_id'],
                        processed_data['amount'],
                        processed_data['currency'],
                        processed_data['transaction_type'],
                        processed_data['status'],
                        processed_data['timestamp']
                    ))
                    conn.commit()
                    print(f"Inserted transaction: {processed_data['transaction_id']}")
                except Exception as e:
                    print(f"Database error: {e}")
                    conn.rollback()
    except KeyboardInterrupt:
        print("Consumer stopped.")
    finally:
        consumer.close()
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()
