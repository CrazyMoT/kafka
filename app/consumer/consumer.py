from confluent_kafka import Consumer, KafkaError
import json
from collections import defaultdict
import time
import threading
import os

# Настройки Kafka
topic = 'user-actions'

# Конфигурация консьюмера
conf = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'),
    'group.id': 'user-action-group',
    'session.timeout.ms': 6000,
    'auto.offset.reset': 'earliest'
}

# Создание экземпляра Kafka Consumer
consumer = Consumer(conf)


# Словарь для хранения действий пользователей
user_actions = defaultdict(lambda: {'page_view': 0, 'button_click': 0})

# Обработка сообщения
def process_message(message):
    try:
        data = json.loads(message.value().decode('utf-8'))
        print(data)
        user_id = data['user_id']
        action_type = data['action_type']
        user_actions[user_id][action_type] += 1
    except json.JSONDecodeError:
        print("Error decoding JSON message")

# Вывод результатов в консоль
def print_user_actions():
    while True:
        time.sleep(10)  # Ожидание 10 секунд
        if user_actions:
            print("----- Results -----")
            for user_id, actions in user_actions.items():
                print(f"User ID: {user_id}, Page Views: {actions['page_view']}, Button Clicks: {actions['button_click']}")
            print("-------------------")
            user_actions.clear()  # Очистка данных после вывода

if __name__ == '__main__':
    # Запуск потока для вывода результатов каждые 10 секунд
    threading.Thread(target=print_user_actions, daemon=True).start()
    try:
        consumer.subscribe([topic])
        while True:
            msg = consumer.poll(1.0)  # Ожидание сообщения в течение 1 секунды
            if msg is None:
                continue
            if msg.error():
                print(msg.error())
                continue
            process_message(msg)
    except Exception as e:
        print(e)
    finally:
        consumer.close()
