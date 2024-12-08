from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from confluent_kafka import Producer
import uvicorn
from loguru import logger

# Конфигурация Kafka Producer
conf = {
    'bootstrap.servers': 'kafka:9092',  # Адрес Kafka
    'client.id': 'python-producer',
    'retries': 3,  # Количество повторных попыток
    'linger.ms': 1000,  # Задержка перед отправкой пакета сообщений
    'acks': 'all',  # Дождаться подтверждения от всех реплик
    'error_cb': lambda err: logger.error(f"Kafka error: {err}")
}

# Создаем объект Producer
producer = Producer(conf)

app = FastAPI()

# Модель данных, которую будет ожидать API
class Message(BaseModel):
    user_id: str
    timestamp: str
    action_type: str
    page_url: str

# Функция для отправки сообщения в Kafka
def delivery_report(err, msg):
    if err is not None:
        logger.info(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Роут для получения данных через POST запрос
@app.post("/send_data")
async def send_data(message: Message):
    try:
        # Преобразуем данные в JSON
        message_json = message.model_dump_json()

        # Отправляем данные в Kafka
        producer.produce('user-actions', value=message_json, callback=delivery_report)
        producer.poll(1)  # Периодическое опрашивание для обработки сообщений
        producer.flush()  # Ожидание завершения отправки

        return {"status": "Message sent to Kafka"}
    except Exception as e:
        logger.error(f"Error sending message: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error sending message: {str(e)}")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5000)
