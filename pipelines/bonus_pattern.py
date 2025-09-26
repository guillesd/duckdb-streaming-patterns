import duckdb
import time

DB_FILE = "events.duckdb"
RAW_VIEW = "raw_events_view"
DEST_VIEW = "user_clicks_view"

"""Bonus Pattern: Streaming with Kafka to DuckDB using Tributary
- Creates streaming views in DuckDB using Tributary to consume messages from a Kafka topic.
- This approach is currently statless and therefore will re-read all messages from the start of the topic on each run.
"""

# Set up DuckDB schema
def create_streaming_views(con: duckdb.DuckDBPyConnection, bootstrap_servers: str, topic: str):
    with con.cursor() as cursor:
        cursor.execute(f"""
            CREATE VIEW IF NOT EXISTS {RAW_VIEW} AS
                SELECT * 
                EXCLUDE message, 
                decode(message)::json AS message 
                FROM tributary_scan_topic('{topic}', "bootstrap.servers" := "{bootstrap_servers}");
        """)
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {DEST_VIEW} AS
                SELECT 
                    user_id,
                    user_name,
                    count(*) AS count_of_clicks,
                    max(timestamp) AS updated_at
                FROM (
                    SELECT 
                        (message ->> '$.timestamp')::timestamp AS timestamp,
                        (message ->> '$.user_id') AS user_id,
                        (message ->> '$.user_name') AS user_name,
                        (message ->> '$.event_type') AS event_type
                    FROM raw_events_view
                ) AS parsed_events
                WHERE event_type = 'CLICK'
                GROUP BY user_id, user_name;
        """)


def query_streaming_view(con: duckdb.DuckDBPyConnection, duration_seconds: int):   
    start_time = time.time()
    with con.cursor() as cursor:
        while time.time() - start_time < duration_seconds:
            try:
                print("Querying top 5 users by click count:")
                cursor.sql(f"SELECT * FROM {DEST_VIEW} ORDER BY count_of_clicks DESC LIMIT 5;").show()
            except Exception as e:
                print("Query error:", e)
        
        time.sleep(1)

def main():
    bootstrap_servers = "localhost:9092"
    topic = "my_topic"
    duration_seconds = 20 #maybe parameterize

    con = duckdb.connect(DB_FILE)

    con.execute("INSTALL tributary FROM community; LOAD tributary;")
    create_streaming_views(con, bootstrap_servers, topic)
    query_streaming_view(con, duration_seconds) # trying to be fancy
    con.close()

if __name__ == "__main__":
    main()
