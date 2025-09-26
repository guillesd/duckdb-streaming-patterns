#!/usr/bin/env python3
import json
import threading
import time
from datetime import datetime
from confluent_kafka import Consumer
import duckdb

"""
Pattern 1.1: Basic Streaming with Kafka and DuckDB
- Consumes JSON messages from a Kafka topic.
- Inserts raw messages into a DuckDB table.
- Periodically aggregates data and updates a summary table.
"""

DB_FILE = "events.duckdb"
RAW_TABLE = "raw_events"
DEST_TABLE = "user_clicks"

# Set up DuckDB schema
def init_db(con: duckdb.DuckDBPyConnection):
    cursor = con.cursor()
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {RAW_TABLE} (
            timestamp TIMESTAMP,
            user_id VARCHAR,
            user_name VARCHAR,
            event_type VARCHAR
        )
    """)
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {DEST_TABLE} (
            user_id VARCHAR,
            user_name VARCHAR,
            count_of_clicks BIGINT,
            updated_at TIMESTAMP
        )
    """)
    cursor.close()


# Kafka consumer thread — inserts raw messages
def consume_and_insert(bootstrap_servers: str, topic: str, con: duckdb.DuckDBPyConnection, duration_seconds: int, group_id: str ="duckdb-consumer"):
    """Consumes messages from Kafka and inserts them into the raw_events table in DuckDB"""
    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest"
    })
    consumer.subscribe([topic])

    print(f"Consuming from topic {topic}...")
    start_time = time.time()

    with con.cursor() as cursor:
        try:
            while time.time() - start_time < duration_seconds:
                msg = consumer.poll(1.0)
                if msg is None:
                    print("No new messages found, sleeping for 5 seconds...")
                    time.sleep(5)
                    continue
                if msg.error():
                    print("Consumer error:", msg.error())
                    continue

                try:
                    event = json.loads(msg.value().decode("utf-8"))
                    ts = datetime.fromisoformat(event["timestamp"])
                    cursor.execute(
                        f"INSERT INTO {RAW_TABLE} VALUES (?, ?, ?, ?)",
                        [ts, event["user_id"], event["user_name"], event["event_type"]]
                    )
                except Exception as e:
                    print("Error inserting:", e)

        except KeyboardInterrupt:
            print("Stopping consumer...")
        finally:
            consumer.close()


# Aggregation thread — runs every 5s
def aggregate_loop(con: duckdb.DuckDBPyConnection, duration_seconds: int):
    """Processes only delta since the last aggregation. In the blog Diagram this is shown as the Delta Processor"""
    
    start_time = time.time()
    with con.cursor() as cursor:
        while time.time() - start_time < duration_seconds:
            try:
                # Determine the latest updated_at in the destination table
                last_updated = cursor.execute(f"SELECT max(updated_at) FROM {DEST_TABLE}").fetchone()[0]

                # Aggregate only new raw data
                aggregate_sql = f"""
                    MERGE INTO {DEST_TABLE} AS dest
                    USING (
                        SELECT 
                            user_id,
                            user_name,
                            COUNT(*) AS count_of_clicks,
                            MAX(timestamp) AS updated_at
                        FROM {RAW_TABLE}
                        WHERE event_type = 'CLICK' AND (? IS NULL OR timestamp > ?)
                        GROUP BY user_id, user_name
                    ) AS src
                    ON dest.user_id = src.user_id
                    WHEN MATCHED THEN 
                        UPDATE SET 
                            count_of_clicks = dest.count_of_clicks + src.count_of_clicks,
                            updated_at = src.updated_at
                    WHEN NOT MATCHED THEN
                        INSERT (user_id, user_name, count_of_clicks, updated_at)
                        VALUES (src.user_id, src.user_name, src.count_of_clicks, src.updated_at)
                """
                cursor.execute(aggregate_sql, [last_updated, last_updated])

                print(f"Aggregation executed at {datetime.now()}")

            except Exception as e:
                print("Aggregation error:", e)

            time.sleep(5)



def main():
    bootstrap_servers = "localhost:9092"
    topic = "my_topic"
    duration_seconds = 40 #maybe parameterize

    con = duckdb.connect(DB_FILE)

    init_db(con)

    t1 = threading.Thread(target=consume_and_insert, args=(bootstrap_servers, topic, con, duration_seconds), daemon=True)
    t2 = threading.Thread(target=aggregate_loop, args=(con, duration_seconds), daemon=True)

    t1.start()
    t2.start()

    t1.join()
    t2.join()


if __name__ == "__main__":
    main()
