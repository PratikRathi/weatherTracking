**The project is designed with the following components:**

* Data Source: We use weatherapi.com API to generate realtime weather data for our pipeline. <br />
* Apache Airflow: Responsible for orchestrating the pipeline and storing fetched data in a PostgreSQL database. <br />
* Apache Kafka and Zookeeper: Used for streaming data from PostgreSQL to the processing engine. <br />
* Control Center and Schema Registry: Helps in monitoring and schema management of our Kafka streams. <br />
* Apache Spark: For data processing with its master and worker nodes. <br />
* Cassandra: Where the processed data will be stored.

**Getting Started**
1) Clone the repository: <br />
git clone https://github.com/PratikRathi/weatherTracking.git

2) Navigate to the project directory: <br />
cd weatherTracking

3) Download the requirements.txt packages: <br />
pip install requirements.txt

4) Run Docker Compose to spin up the services: <br />
docker-compose up

5) Run the spark_stream python file: <br />
python3 spark_stream.py



