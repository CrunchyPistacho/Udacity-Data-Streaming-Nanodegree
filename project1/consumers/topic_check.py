from confluent_kafka.admin import AdminClient


def topic_exists(topic):
    """Checks if the given topic exists in Kafka"""
    client = AdminClient({"bootstrap.servers": "PLAINTEXT://localhost:9092,PLAINTEXT://localhost:9093,PLAINTEXT://localhost:9094"})
    topic_metadata = client.list_topics(timeout=5)
    return topic in set(t.topic for t in iter(topic_metadata.topics.values()))
