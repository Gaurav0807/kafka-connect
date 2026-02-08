FROM python:3.11-slim

WORKDIR /app

# System deps (librdkafka for confluent-kafka)
RUN apt-get update && apt-get install -y \
    gcc \
    librdkafka-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .

CMD ["python", "producer.py"]
