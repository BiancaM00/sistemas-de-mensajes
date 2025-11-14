FROM python:3.13-slim

WORKDIR /app

# Instalamos dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiamos el código del productor
COPY producer.py .

CMD ["python", "producer.py"]
