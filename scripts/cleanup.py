from confluent_kafka.admin import AdminClient
import os
import shutil

def delete_topic(bootstrap_servers: str, topic: str):
    """
    Delete a Kafka topic.
    """
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    fs = admin.delete_topics([topic], operation_timeout=30)

    for t, f in fs.items():
        try:
            f.result()  # The result itself is None
            print(f"Topic '{t}' successfully marked for deletion.")
        except Exception as e:
            print(f"Failed to delete topic {t}: {e}")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Delete a Kafka topic.")
    parser.add_argument("--bootstrap-servers", type=str, required=True, help="Kafka bootstrap servers")
    parser.add_argument("--topic", type=str, required=True, help="Kafka topic to delete")
    args = parser.parse_args()

    FILES_TO_CLEAN = ["events.duckdb", "events.duckdb.wal", "events_ducklake.db"]
    for file in FILES_TO_CLEAN:
        if os.path.exists(file):
            os.remove(file)
            print(f"Deleted file: {file}")
        else:
            print(f"File not found, skipping deletion: {file}")

    DIRS_TO_CLEAN = ["events_ducklake.db.files"]
    for dir in DIRS_TO_CLEAN:
        if os.path.exists(dir):
            shutil.rmtree(dir)
            print(f"Deleted directory: {dir}")
        else:
            print(f"Directory not found, skipping deletion: {dir}")

    delete_topic(args.bootstrap_servers, args.topic)
