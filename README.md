# DuckDB Streaming Patterns

This repository demonstrates several patterns for streaming data from Kafka into DuckDB (and DuckLake) using Python and Spark. It provides practical examples for ingesting, processing, and aggregating event data in real time, suitable for analytics and prototyping modern data pipelines. For a full write-up and context read the blogpost TODDO.

## Repository Structure

- [`pipelines/pattern_1_1.py`](pipelines/pattern_1_1.py): Basic streaming from Kafka to DuckDB with periodic aggregation.
- [`pipelines/pattern_1_2.py`](pipelines/pattern_1_2.py): Streaming from Kafka to DuckLake with Change Data Feed aggregation.
- [`pipelines/pattern_2.py`](pipelines/pattern_2.py): Streaming from Kafka to DuckDB using Spark Structured Streaming.
- [`pipelines/bonus_pattern.py`](pipelines/bonus_pattern.py): Streaming views in DuckDB using the Tributary extension.
- [`scripts/producer.py`](scripts/producer.py): Produces random user event messages to a Kafka topic.
- [`scripts/cleanup.py`](scripts/cleanup.py): Cleans up local DuckDB/DuckLake files and deletes Kafka topics.

## Getting Started

### Prerequisites

- Python 3.8+
- Docker (for running Kafka)
- Java (for Spark)
- Install dependencies:
  ```
  pip install -r requirements.txt
  ```

### Start Kafka Broker

Start the Kafka broker using Docker Compose:
```
docker compose up -d
```

### Produce Events

Run the producer to generate random user events:
```
python scripts/producer.py --bootstrap-servers localhost:9092 --topic my_topic --duration 60
```

### Run a Streaming Pattern

Example: Run Pattern 1.1 (DuckDB streaming and aggregation)
```
python pipelines/pattern_1_1.py
```

Other patterns can be run similarly:
- DuckLake: `python pipelines/pattern_1_2.py`
- Spark: `python pipelines/pattern_2.py`
- Tributary: `python pipelines/bonus_pattern.py`

### Cleanup

To remove generated databases and Kafka topics:
```
python scripts/cleanup.py --bootstrap-servers localhost:9092 --topic my_topic
```

## Notes

- The default Kafka topic is `my_topic`. You can change this via command-line arguments.
- DuckLake and Tributary extensions are installed automatically by the scripts.
