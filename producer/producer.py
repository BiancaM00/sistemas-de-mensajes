import os
import json
import time
import random
import logging
from datetime import datetime, timezone
import pika

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER] %(levelname)s: %(message)s"
)

# Variables de entorno del docker-compose
RABBIT_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBIT_USER = os.getenv("RABBITMQ_USER", "rabbit")
RABBIT_PASS = os.getenv("RABBITMQ_PASS", "rabbitpass")
STATION_ID = os.getenv("STATION_ID", "station-001")

EXCHANGE_NAME = "weather"
ROUTING_KEY = f"station.{STATION_ID}"


def connect():
    """Crea conexión a RabbitMQ con reintentos."""
    credentials = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
    params = pika.ConnectionParameters(
        host=RABBIT_HOST,
        credentials=credentials,
        heartbeat=60,
        blocked_connection_timeout=30
    )
    return pika.BlockingConnection(params)


def main():
    while True:
        try:
            logging.info("Conectando a RabbitMQ…")
            conn = connect()
            ch = conn.channel()

            ch.exchange_declare(
                exchange=EXCHANGE_NAME,
                exchange_type="topic",
                durable=True
            )

            logging.info("Productor conectado. Enviando datos…")

            while True:
                payload = {
                    "station_id": STATION_ID,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "temperature": round(random.uniform(-10, 40), 2),
                    "humidity": round(random.uniform(0, 100), 2),
                    "pressure": round(random.uniform(950, 1050), 2),
                }

                body = json.dumps(payload)

                ch.basic_publish(
                    exchange=EXCHANGE_NAME,
                    routing_key=ROUTING_KEY,
                    body=body,
                    properties=pika.BasicProperties(
                        content_type="application/json",
                        delivery_mode=2  # persistente
                    )
                )

                logging.info("Mensaje enviado: %s", body)
                time.sleep(2)

        except Exception as e:
            logging.error("Error productor: %s", e)
            logging.info("Reintentando conexión en 5 segundos…")
            time.sleep(5)


if __name__ == "__main__":
    main()
