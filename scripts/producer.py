#!/usr/bin/env python3
import argparse
import time
import uuid
import random
import json
from datetime import datetime
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic


# Pre-generate 40 users with stable IDs
USER_NAMES = [
    "Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Hank",
    "Ivy", "Jack", "Karen", "Leo", "Mona", "Nate", "Olivia", "Paul",
    "Quincy", "Rita", "Sam", "Tina", "Uma", "Victor", "Wendy", "Xavier",
    "Yara", "Zack", "Aiden", "Bella", "Caleb", "Daisy", "Ethan", "Fiona",
    "Gavin", "Hazel", "Isaac", "Jade", "Kyle", "Luna", "Mason", "Nora"
]
USER_IDS = {name: str(uuid.uuid4()) for name in USER_NAMES}

EVENT_TYPES = ["CLICK", "IMPRESSION", "HOVER_MOUSE"]


def create_topic_if_not_exists(bootstrap_servers: str, topic: str, num_partitions: int = 1, replication_factor: int = 1):
    """
    Create a Kafka topic if it does not already exist.
    """
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    cluster_metadata = admin.list_topics(timeout=5)

    if topic in cluster_metadata.topics:
        print(f"Topic '{topic}' already exists.")
        return

    print(f"Creating topic '{topic}'...")
    new_topic = NewTopic(topic, num_partitions=num_partitions, replication_factor=replication_factor)
    fs = admin.create_topics([new_topic])

    for t, f in fs.items():
        try:
            f.result()
            print(f"Topic '{t}' created.")
        except Exception as e:
            print(f"Failed to create topic {t}: {e}")


def delivery_report(err, msg):
    """Callback called once for each produced message to report delivery result."""
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"Record produced to {msg.topic()} partition [{msg.partition()}] @ offset {msg.offset()}")
        # print(msg.value().decode("utf-8"))


def produce_messages(bootstrap_servers: str, topic: str, duration_seconds: int):
    """
    Produce JSON messages to Kafka for the given duration.
    """
    producer = Producer({"bootstrap.servers": bootstrap_servers})
    start_time = time.time()

    while time.time() - start_time < duration_seconds:
        # Pick a random user
        user_name = random.choice(USER_NAMES)
        user_id = USER_IDS[user_name]

        # Pick a random event
        event_type = random.choice(EVENT_TYPES)

        # Build the message
        message = {
            "timestamp": datetime.now().isoformat(),
            "user_id": user_id,
            "user_name": user_name,
            "event_type": event_type
        }

        # Produce JSON message
        producer.produce(
            topic,
            key=user_id.encode("utf-8"),
            value=json.dumps(message).encode("utf-8"),
            callback=delivery_report
        )
        producer.poll(0)  # Trigger callbacks

        time.sleep(0.01)  # Adjust rate of message production if desired

    producer.flush()
    print("Finished producing messages.")


def main():
    parser = argparse.ArgumentParser(description="Kafka JSON Producer Example")
    parser.add_argument("--bootstrap-servers", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="test-topic", help="Kafka topic to produce to")
    parser.add_argument("--duration", type=int, default=300, help="Duration in seconds (default: 300 = 5 minutes)")
    args = parser.parse_args()

    create_topic_if_not_exists(args.bootstrap_servers, args.topic)
    produce_messages(args.bootstrap_servers, args.topic, args.duration)


if __name__ == "__main__":
    main()
