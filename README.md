# Retail Data Streaming Pipeline

This project demonstrates a real-time data streaming pipeline using Kafka and MinIO for a retail dataset.

## Architecture

- **Kafka**: Message broker for real-time data streaming
- **MinIO**: S3-compatible object storage
- **Kafka UI**: Web interface for Kafka monitoring
- **Producer**: Reads data from CSV and sends to Kafka
- **Consumer**: Processes Kafka messages and stores in MinIO

## Prerequisites

- Docker and Docker Compose
- Python 3.6+
- Required Python packages:
  - kafka-python
  - pandas
  - boto3

## Setup

1. Install required Python packages:

```bash
pip install kafka-python pandas boto3
```

2. Start the infrastructure services:

```bash
docker-compose up -d
```

3. Verify services are running:

```bash
docker ps
```

4. Check Kafka connection:

```bash
python check_kafka.py
```

5. Create the Kafka topic:

```bash
python create_topic.py
```

## Web Interfaces

- **Kafka UI**: http://localhost:8080
- **MinIO Console**: http://localhost:9001 (login with minioadmin/minioadmin)

## Running the Pipeline

1. Start the consumer in one terminal:

```bash
python consumer.py
```

2. Start the producer in another terminal:

```bash
python producer.py
```

## Troubleshooting

### Kafka UI shows no data

Make sure:
1. All containers are running: `docker ps`
2. Kafka is accessible: `python check_kafka.py` 
3. Topic is created: `python create_topic.py`
4. Check Docker logs: `docker logs kafka`

### Producer/Consumer Connection Issues

- Ensure Docker containers are running
- Check network settings in docker-compose.yml
- Verify localhost:9092 is accessible

### MinIO Issues

- Verify MinIO is running: http://localhost:9001
- Check credentials (minioadmin/minioadmin)
- Look for errors in consumer logs

## File Description

- `docker-compose.yml`: Infrastructure configuration
- `producer.py`: Sends retail data to Kafka
- `consumer.py`: Processes Kafka messages and stores in MinIO
- `check_kafka.py`: Utility to verify Kafka connection
- `create_topic.py`: Creates required Kafka topic 