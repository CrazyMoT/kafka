from confluent_kafka.admin import AdminClient, NewTopic

def create_topic(broker, topic_name, num_partitions=3, replication_factor=1):
    admin_client = AdminClient({"bootstrap.servers": broker})
    topic = NewTopic(topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
    try:
        admin_client.create_topics([topic])
        print(f"Topic '{topic_name}' created successfully")
    except Exception as e:
        print(f"Failed to create topic: {e}")

if __name__ == "__main__":
    create_topic("kafka:9092", "user-actions")
