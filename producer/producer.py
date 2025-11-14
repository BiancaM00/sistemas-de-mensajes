import os, time, json, logging, random
from datetime import datetime, timezone
import pika

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

RABBIT_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBIT_USER = os.getenv("RABBITMQ_USER", "user")
RABBIT_PASS = os.getenv("RABBITMQ_PASS", "password")
EXCHANGE = os.getenv("EXCHANGE", "weather")
EXCHANGE_TYPE = "topic"
ROUTING_KEY = "station.data"

def connect():
    credentials = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
    params = pika.ConnectionParameters(host=RABBIT_HOST, credentials=credentials, heartbeat=60)
    return pika.BlockingConnection(params)

def ensure_exchange(ch):
    ch.exchange_declare(exchange=EXCHANGE, exchange_type=EXCHANGE_TYPE, durable=True)

def publish_sample_messages(ch, count=100, delay=1.0):
    stations = ["ST-001", "ST-002", "ST-003"]
    for i in range(count):
        payload = {
            "station_id": random.choice(stations),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "temperature_c": round(random.uniform(-20, 45), 2),
            "humidity_pct": round(random.uniform(0, 100), 2),
            "pressure_hpa": round(random.uniform(900, 1100), 2)
        }
        body = json.dumps(payload)
        properties = pika.BasicProperties(delivery_mode=2, content_type="application/json")  # persistent
        ch.basic_publish(exchange=EXCHANGE, routing_key=ROUTING_KEY, body=body, properties=properties)
        logging.info("Published message: %s", body)
        time.sleep(delay)

def main():
    backoff = 1
    while True:
        try:
            conn = connect()
            ch = conn.channel()
            ensure_exchange(ch)
            ch.queue_declare(queue="weather_queue", durable=True)
            ch.queue_bind(queue="weather_queue", exchange=EXCHANGE, routing_key="station.#")
            logging.info("Connected to RabbitMQ, starting to publish")
            publish_sample_messages(ch, count=200, delay=2.0)
            conn.close()
            break
        except Exception as e:
            logging.exception("Producer connection/publish error, retrying in %s seconds", backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)

if __name__ == '__main__':
    main()
