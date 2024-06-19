The project is designed with the following components:

Data Source: We use weatherapi.com API to generate realtime weather data for our pipeline.
Apache Airflow: Responsible for orchestrating the pipeline and storing fetched data in a PostgreSQL database.
Apache Kafka and Zookeeper: Used for streaming data from PostgreSQL to the processing engine.
Control Center and Schema Registry: Helps in monitoring and schema management of our Kafka streams.
Apache Spark: For data processing with its master and worker nodes.
Cassandra: Where the processed data will be stored.

Getting Started 
1) Clone the repository:
git clone https://github.com/PratikRathi/weatherTracking.git

2) Navigate to the project directory:
cd weatherTracking

3) Run Docker Compose to spin up the services:
docker-compose up



