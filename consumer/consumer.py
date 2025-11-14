#!/usr/bin/env python3
import os, time, json, logging
from datetime import datetime, timezone
import pika
import psycopg2
from psycopg2.extras import Json

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

RABBIT_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBIT_USER = os.getenv("RABBITMQ_USER", "user")
RABBIT_PASS = os.getenv("RABBITMQ_PASS", "password")
EXCHANGE = os.getenv("EXCHANGE", "weather")
QUEUE = os.getenv("QUEUE", "weather_queue")

PGHOST = os.getenv("PGHOST", "localhost")
PGUSER = os.getenv("PGUSER", "weather")
PGPASSWORD = os.getenv("PGPASSWORD", "weatherpass")
PGDATABASE = os.getenv("PGDATABASE", "weatherdb")

VALID_RANGES = {
    "temperature_c": (-100.0, 100.0),
    "humidity_pct": (0.0, 100.0),
    "pressure_hpa": (300.0, 1200.0)
}

def pg_connect(retries=5, delay=2):
    for attempt in range(retries):
        try:
            conn = psycopg2.connect(host=PGHOST, user=PGUSER, password=PGPASSWORD, dbname=PGDATABASE)
            conn.autocommit = True
            logging.info("Connected to Postgres")
            return conn
        except Exception as e:
            logging.exception("Postgres connection failed, retrying... (%s/%s)", attempt+1, retries)
            time.sleep(delay)
    raise RuntimeError("Could not connect to Postgres")

def insert_log(pg_conn, payload):
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO weather_logs (station_id, timestamp, temperature_c, humidity_pct, pressure_hpa, raw_payload)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                payload.get("station_id"),
                payload.get("timestamp"),
                payload.get("temperature_c"),
                payload.get("humidity_pct"),
                payload.get("pressure_hpa"),
                Json(payload)
            )
        )

def validate(payload):
    errors = []
    for key, (low, high) in VALID_RANGES.items():
        if key in payload:
            try:
                v = float(payload[key])
                if v < low or v > high:
                    errors.append(f"{key} out of range: {v} not in [{low},{high}]")
            except Exception:
                errors.append(f"{key} invalid type/value: {payload.get(key)}")
    return errors

def connect_rabbit():
    credentials = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
    params = pika.ConnectionParameters(host=RABBIT_HOST, credentials=credentials, heartbeat=60)
    return pika.BlockingConnection(params)

def main():
    backoff = 1
    pg_conn = pg_connect(retries=10, delay=2)
    while True:
        try:
            conn = connect_rabbit()
            ch = conn.channel()
            ch.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
            ch.queue_declare(queue=QUEUE, durable=True)
            ch.queue_bind(queue=QUEUE, exchange=EXCHANGE, routing_key="station.#")
            ch.basic_qos(prefetch_count=1)
            logging.info("Consumer connected and waiting for messages")

            def callback(ch, method, properties, body):
                logging.info("Received message: %s", body)
                try:
                    payload = json.loads(body)
                except Exception as e:
                    logging.exception("Invalid JSON payload, rejecting: %s", e)
                    ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
                    return

                errors = validate(payload)
                if errors:
                    logging.error("Validation errors: %s", errors)
                    # Log and ack so poison messages don't block pipeline; in prod se coloca DLQ
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    return

                try:
                    insert_log(pg_conn, payload)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    logging.info("Inserted and acked message")
                except Exception as e:
                    logging.exception("DB insert failed, will nack+requeue")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

            ch.basic_consume(queue=QUEUE, on_message_callback=callback, auto_ack=False)
            ch.start_consuming()
        except Exception as e:
            logging.exception("Consumer connection error, reconnecting in %s seconds", backoff)
            time.sleep(backoff)
            backoff = min(60, backoff * 2)

if __name__ == '__main__':
    main()
