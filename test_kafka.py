import pytest
import asyncio
import random
import time
import aiohttp
from aiohttp.client_exceptions import ClientError


USER_IDS = [1, 2, 3]
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
async def send_to_producer(message_json):
    async with aiohttp.ClientSession() as session:
        try:
            headers = {'Content-Type': 'application/json'}
            async with session.post(API_URL, json=message_json, headers=headers) as response:
                response.raise_for_status()
                return response
        except ClientError as e:
            print(f"Request failed: {e}")
            return None


# Тест корректной работы отправки данных через API
@pytest.mark.asyncio
async def test_send_data_to_producer():
    # Генерация случайного действия пользователя
    message = generate_user_action()

    response = await send_to_producer(message)
    assert response is not None, "Failed to send data to API"
    assert response.status == 200, f"Unexpected response code: {response.status}"



# Тест на обработку ошибки при отправке данных через API
@pytest.mark.asyncio
async def test_send_invalid_data():
    # Отправляем данные неправильного формата (например, строка вместо JSON)
    invalid_message = "This is not a valid JSON message"
    response = await send_to_producer(invalid_message)
    assert response is None, "Expected failure when sending invalid data"


# Тест на производительность (отправка большого количества сообщений)
@pytest.mark.asyncio
async def test_send_multiple_messages():
    num_messages = 1000
    messages = [generate_user_action() for _ in range(num_messages)]

    # Асинхронная отправка сообщений одновременно
    tasks = [send_to_producer(message) for message in messages]
    responses = await asyncio.gather(*tasks)

    # Проверка всех ответов
    for response in responses:
        assert response is not None, "Failed to send message to API"
        assert response.status == 200, f"Unexpected response code: {response.status}"

