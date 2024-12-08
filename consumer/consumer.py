from confluent_kafka import Consumer, KafkaException, KafkaError
import json
from collections import defaultdict
import time
import threading
from loguru import logger


# Настройки Kafka
topic = 'user-actions'

# Конфигурация консьюмера
conf = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'user-action-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,  # Автоматический коммит оффсетов
    'session.timeout.ms': 10000,  # Таймаут сессии
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
        logger.error("Error decoding JSON message")

# Вывод результатов в консоль
def print_user_actions():
    while True:
        time.sleep(10)  # Ожидание 10 секунд
        if user_actions:
            logger.info("----- Results -----")
            for user_id, actions in user_actions.items():
                logger.info(f"User ID: {user_id}, Page Views: {actions['page_view']}, Button Clicks: {actions['button_click']}")
            logger.info("-------------------")
            user_actions.clear()  # Очистка данных после вывода

def main():
    # Запуск потока для вывода результатов каждые 10 секунд
    threading.Thread(target=print_user_actions, daemon=True).start()
    try:
        consumer.subscribe([topic])
        while True:
            msg = consumer.poll(1.0)  # Ожидание сообщения в течение 1 секунды
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info(f"End of partition reached {msg.partition()}")
                else:
                    logger.error(f"Error consuming message: {msg.error()}")
                    raise KafkaException(msg.error())
            else:
                process_message(msg)
    except Exception as e:
        print(e)
    finally:
        consumer.close()

if __name__ == '__main__':
    main()
