import os
import json
import time
import logging
from datetime import datetime, timezone
import pika
import psycopg2
from psycopg2.extras import Json

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CONSUMER] %(levelname)s: %(message)s"
)

# RabbitMQ
RABBIT_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBIT_USER = os.getenv("RABBITMQ_USER", "rabbit")
RABBIT_PASS = os.getenv("RABBITMQ_PASS", "rabbitpass")

EXCHANGE_NAME = "weather"
QUEUE_NAME = "weather_logs_queue"
ROUTING_KEY = "station.#"

# PostgreSQL
POST_HOST = os.getenv("POSTGRES_HOST", "postgres")
POST_DB = os.getenv("POSTGRES_DB", "weather")
POST_USER = os.getenv("POSTGRES_USER", "pguser")
POST_PASS = os.getenv("POSTGRES_PASS", "pgpass")


def connect_db():
    while True:
        try:
            conn = psycopg2.connect(
                host=POST_HOST,
                dbname=POST_DB,
                user=POST_USER,
                password=POST_PASS
            )
            conn.autocommit = True
            logging.info("Conectado a PostgreSQL")
            return conn
        except Exception as e:
            logging.error("Error conectando a PostgreSQL: %s", e)
            time.sleep(5)


db_conn = None


def save(payload, status, error_msg=None):
    global db_conn
    if not db_conn or db_conn.closed:
        db_conn = connect_db()

    try:
        with db_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO weather_logs (
                    station_id, timestamp,
                    temperature, humidity, pressure,
                    status, error_message, raw_payload
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    payload["station_id"],
                    datetime.fromisoformat(payload["timestamp"]),
                    payload["temperature"],
                    payload["humidity"],
                    payload["pressure"],
                    status,
                    error_msg,
                    Json(payload)
                )
            )
    except Exception as e:
        logging.error("Error guardando en DB: %s", e)
        db_conn = None


def validate(payload):
    errors = []

    if not (-50 <= payload["temperature"] <= 60):
        errors.append("Temperatura fuera de rango")

    if not (0 <= payload["humidity"] <= 100):
        errors.append("Humedad fuera de rango")

    if not (300 <= payload["pressure"] <= 1100):
        errors.append("Presión fuera de rango")

    return errors


def connect_rabbit():
    credentials = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
    params = pika.ConnectionParameters(
        host=RABBIT_HOST,
        credentials=credentials,
        heartbeat=60
    )
    return pika.BlockingConnection(params)


def callback(ch, method, properties, body):
    try:
        payload = json.loads(body)
    except:
        logging.error("Mensaje no es JSON válido")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    logging.info("Mensaje recibido: %s", payload)

    errors = validate(payload)

    if errors:
        save(payload, "error", "; ".join(errors))
        logging.warning("Errores: %s", errors)
    else:
        save(payload, "ok", None)

    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    while True:
        try:
            conn = connect_rabbit()
            ch = conn.channel()

            ch.exchange_declare(
                exchange=EXCHANGE_NAME,
                exchange_type="topic",
                durable=True
            )

            ch.queue_declare(queue=QUEUE_NAME, durable=True)
            ch.queue_bind(queue=QUEUE_NAME, exchange=EXCHANGE_NAME, routing_key=ROUTING_KEY)

            ch.basic_qos(prefetch_count=1)
            ch.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)

            logging.info("Consumer iniciado, esperando mensajes...")
            ch.start_consuming()

        except Exception as e:
            logging.error("Error en consumer: %s", e)
            time.sleep(5)


if __name__ == "__main__":
    db_conn = connect_db()
    main()
