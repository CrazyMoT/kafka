from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from confluent_kafka import Producer
import json
import uvicorn

# Конфигурация Kafka Producer
conf = {
    'bootstrap.servers': 'kafka:9092',  # Адрес Kafka
    'client.id': 'python-producer'
}

# Создаем объект Producer
producer = Producer(conf)

# FastAPI приложение
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
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Роут для получения данных через POST запрос
@app.post("/send_data")
async def send_data(message: Message):
    try:
        # Преобразуем данные в JSON
        message_dict = message.dict()
        message_json = json.dumps(message_dict)

        # Отправляем данные в Kafka
        producer.produce('user-actions', value=message_json, callback=delivery_report)
        producer.flush()  # Ожидаем завершения отправки

        return {"status": "Message sent to Kafka"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error sending message: {str(e)}")

# Для тестирования
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5000)
