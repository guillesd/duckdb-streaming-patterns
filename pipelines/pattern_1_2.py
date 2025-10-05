#!/usr/bin/env python3
import json
import threading
import time
from datetime import datetime
from confluent_kafka import Consumer
import duckdb
import argparse

"""
Pattern 1.2: Basic Streaming with Kafka and DuckLake
- Consumes JSON messages from a Kafka topic.
- Inserts raw messages into a DuckLake table.
- Periodically aggregates data and updates a summary table using Change Data Feed from DuckLake.
"""

RAW_TABLE = "raw_events"
DEST_TABLE = "user_clicks"

# Set up DuckDB schema
def init_db(con: duckdb.DuckDBPyConnection):
    cursor = con.cursor()
    cursor.execute("USE events_ducklake;")
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
            last_snapshot INT,        
        )
    """)
    cursor.close()


# Kafka consumer thread — inserts raw messages
def consume_and_insert(bootstrap_servers: str, topic: str, con: duckdb.DuckDBPyConnection, duration_seconds: int, group_id: str ="duckdb-consumer"):
    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest"
    })
    consumer.subscribe([topic])

    print(f"Consuming from topic {topic}...")
    start_time = time.time()

    with con.cursor() as cursor:
        cursor.execute("USE events_ducklake;")
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
            print("Closing consumer...")
            consumer.close()
    

# Aggregation thread — runs every 5s
def aggregate_loop(con: duckdb.DuckDBPyConnection, duration_seconds: int):    
    start_time = time.time()
    with con.cursor() as cursor:
        cursor.execute("USE events_ducklake;")
        while time.time() - start_time < duration_seconds:
            try:
                # Determine the latest last_snapshot in the destination table
                last_snapshot_update = cursor.execute(f"SELECT max(last_snapshot) FROM {DEST_TABLE}").fetchone()[0] or 0
                max_snapshot = cursor.execute(f"SELECT max(snapshot_id) FROM events_ducklake.snapshots();").fetchone()[0]

                # Aggregate only new raw data
                aggregate_sql = f"""
                    MERGE INTO {DEST_TABLE} AS dest
                    USING (
                        SELECT 
                            user_id,
                            user_name,
                            COUNT(*) AS count_of_clicks,
                            ? AS last_snapshot,
                        FROM events_ducklake.table_changes('{RAW_TABLE}', ?, ?)
                        WHERE event_type = 'CLICK'
                        GROUP BY user_id, user_name
                    ) AS src
                    ON dest.user_id = src.user_id
                    WHEN MATCHED THEN 
                        UPDATE SET 
                            count_of_clicks = dest.count_of_clicks + src.count_of_clicks,
                            last_snapshot = src.last_snapshot
                    WHEN NOT MATCHED THEN
                        INSERT (user_id, user_name, count_of_clicks, last_snapshot)
                        VALUES (src.user_id, src.user_name, src.count_of_clicks, src.last_snapshot)
                """
                cursor.execute(aggregate_sql, [max_snapshot, last_snapshot_update, max_snapshot])

                print(f"Aggregation executed at {datetime.now()} from {last_snapshot_update} to {max_snapshot}")
                time.sleep(5)

            except Exception as e:
                print("Aggregation error:", e)

        


def main():
    parser = argparse.ArgumentParser(description="Kafka to DuckDB streaming pipeline")
    parser.add_argument("--bootstrap-servers", type=str, default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", type=str, default="my_topic", help="Kafka topic to consume from")
    parser.add_argument("--duration-seconds", type=int, default=20, help="Duration to run the pipeline (seconds)")
    args = parser.parse_args()

    con = duckdb.connect(config = {"allow_unsigned_extensions": "true"})
    con.execute("FORCE INSTALL ducklake; LOAD ducklake;")
    con.execute("ATTACH 'ducklake:events_ducklake.db' AS events_ducklake (DATA_INLINING_ROW_LIMIT 10);")
    init_db(con)

    t1 = threading.Thread(target=consume_and_insert, args=(args.bootstrap_servers, args.topic, con, args.duration_seconds))
    t2 = threading.Thread(target=aggregate_loop, args=(con, args.duration_seconds), daemon=True)

    t1.start()
    t2.start()

    t1.join()
    t2.join()

    # Final flush to ensure all inlined data is persisted to Parquet files
    print("Flushing inlined data to Parquet files...")
    con.execute(f"CALL ducklake_flush_inlined_data('events_ducklake', table_name => '{RAW_TABLE}');")
    print("Compacting files...")
    con.execute(f"CALL ducklake_rewrite_data_files('events_ducklake', '{DEST_TABLE}');")
    con.execute(f"CALL ducklake_merge_adjacent_files('events_ducklake', '{DEST_TABLE}');")
    con.close()

if __name__ == "__main__":
    main()
