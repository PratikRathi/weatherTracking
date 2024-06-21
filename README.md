**Project Architecture**

![Project Architecture](https://github.com/PratikRathi/weatherTracking/blob/main/Architecture%20Diagram.png)

**Project Description**

To process the data in real-time, we will utilize Apache Kafka and Spark Streaming. 
Kafka will serve as the messaging system, streaming data from the IoT sensors to Spark Streaming.

Spark Streaming will handle real-time data processing, perform analytics, and output the results to Cassandra.

For data visualization and analysis, we will use Tableau. 
Tableau will connect to Cassandra, generating interactive visualizations that provide insights into weather patterns and trends.

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

4) Download the required JAR files and move them into the jars folder: <br />
spark-sql-kafka-0-10_2.12-3.4.1.jar <br />
spark-token-provider-kafka-0-10_2.12-3.4.1.jar <br />
kafka-clients-3.4.1.jar <br />
commons-pool2-2.11.1.jar

5) Run Docker Compose to spin up the services: <br />
docker-compose up

6) Run the spark_stream python file: <br />
python3 spark_stream.py

**Dashboarding**

![Tableau Weather Visualization](https://github.com/PratikRathi/weatherTracking/blob/main/Weather%20Reporting%20Tableau.png)

**Reference**

* Same architecture with different usecase [YouTube Video Tutorial](https://www.youtube.com/watch?v=GqAcTrqKcrY). <br />
* https://github.com/HelloSongi/Spark-Structured-streaming-IoT-Weather-Sensors/blob/main/README.md


