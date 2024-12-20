# Kafka Streaming Project

This project uses Kafka and Spark to stream and process data from the Random User API (https://randomuser.me/), storing results in PostgreSQL.

## Features
- Kafka producer fetches user data and streams it to a topic.
- Spark consumer processes the topic and saves data to PostgreSQL.

## How to Run
1. Activate the virtual environment.
2. Run the Kafka and PostgreSQL servers.
3. Run the producer and consumer scripts.

## Requirements
- Python 3.11 (Some packages may not be compatible with other versions)
- Kafka, Spark, PostgreSQL