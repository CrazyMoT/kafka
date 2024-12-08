FROM python:3.11-slim

WORKDIR /app

RUN pip install confluent_kafka loguru

COPY . .

CMD ["python", "consumer.py"]
