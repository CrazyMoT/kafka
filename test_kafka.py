import pytest
import json
import random
import time
import requests
from confluent_kafka import Consumer
from requests.exceptions import RequestException

USER_IDS = ["user1", "user2", "user3"]
PAGE_URLS = ["https://example.com/page1", "https://example.com/page2", "https://example.com/page3"]
ACTION_TYPES = ["page_view", "button_click"]
API_URL = "http://localhost:5000/send_data"
KAFKA_TOPIC = "test_topic"


# Генерация случайных действий пользователя
def generate_user_action():
    return {
        "user_id": random.choice(USER_IDS),
        "timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        "action_type": random.choice(ACTION_TYPES),
        "page_url": random.choice(PAGE_URLS)
    }


# Отправка сообщения через API (который передает данные в продюсер)
def send_to_producer(message_json):
    try:
        headers = {'Content-Type': 'application/json'}
        response = requests.post(API_URL, json=message_json, headers=headers)
        response.raise_for_status()
        return response
    except RequestException as e:
        print(f"Request failed: {e}")
        return None


# Тест корректной работы отправки данных через API
@pytest.mark.asyncio
async def test_send_data_to_producer_and_consumer():
    # Генерация случайного действия пользователя
    message = generate_user_action()

    # Отправка данных через API (это вызовет продюсер, который отправит их в Kafka)
    response = send_to_producer(message)
    assert response is not None, "Failed to send data to API"
    assert response.status_code == 200, f"Unexpected response code: {response.status_code}"



# Тест на обработку ошибки при отправке данных через API
@pytest.mark.asyncio
async def test_send_invalid_data():
    # Отправляем данные неправильного формата (например, строка вместо JSON)
    invalid_message = "This is not a valid JSON message"
    response = send_to_producer(invalid_message)
    assert response is None, "Expected failure when sending invalid data"


# Тест на производительность (отправка большого количества сообщений)
@pytest.mark.asyncio
async def test_send_multiple_messages():
    num_messages = 1000
    messages = [generate_user_action() for _ in range(num_messages)]

    # Отправляем 1000 сообщений
    for message in messages:
        response = send_to_producer(message)
        assert response is not None, "Failed to send message to API"
        assert response.status_code == 200, f"Unexpected response code: {response.status_code}"

    # Подключаемся к Kafka Consumer
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'test_group',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([KAFKA_TOPIC])

    # Ожидаем и получаем все сообщения
    received_messages = []
    for _ in range(num_messages):
        msg = consumer.poll(timeout=2.0)
        if msg is not None and not msg.error():
            received_messages.append(json.loads(msg.value()))

    assert len(received_messages) == num_messages, "Not all messages received by consumer"

    # Проверяем, что каждое сообщение, отправленное в API, получено через Kafka
    for original, received in zip(messages, received_messages):
        assert original == received, "Mismatch between sent and received message"

    # Закрываем консьюмера
    consumer.close()
