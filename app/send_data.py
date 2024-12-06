import json
import random
import time
import requests

USER_IDS = ["user1", "user2", "user3"]
PAGE_URLS = ["https://example.com/page1", "https://example.com/page2", "https://example.com/page3"]
ACTION_TYPES = ["page_view", "button_click"]

# URL FastAPI-приложения
API_URL = "http://localhost:5000/send_data"  # Здесь укажите адрес вашего FastAPI-приложения

def generate_user_action():
    # Генерация случайных данных
    user_action = {
        "user_id": random.choice(USER_IDS),
        "timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        "action_type": random.choice(ACTION_TYPES),
        "page_url": random.choice(PAGE_URLS)
    }
    return user_action

def send_to_producer(message_json):
    # Отправка данных в FastAPI-приложение
    headers = {'Content-Type': 'application/json'}
    response = requests.post(API_URL, json=message_json, headers=headers)

    if response.status_code == 200:
        print(f"Message sent successfully: {response.json()}")
    else:
        print(f"Failed to send message: {response.status_code}, {response.text}")

if __name__ == '__main__':
    for _ in range(10):  # Генерация 10 сообщений для примера
        user_action = generate_user_action()
        send_to_producer(user_action)
        time.sleep(1)  # Пауза в 1 секунду между сообщениями
