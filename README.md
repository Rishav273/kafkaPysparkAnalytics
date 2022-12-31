# kafkaPysparkAnalytics


## ETL data pipeline that uses Apache Kafka and Pyspark structured streaming to stream credit card transactions data in real time and write them to a file sink / console as per requirement.

### NOTE: The following instructions apply to a Linux system only.

Prerequisites:
* Ensure Java 1.8 is installed in your system and the environment variables are added correctly.
https://www.oracle.com/in/java/technologies/javase/javase8-archive-downloads.html

* Download spark 3.0.0 from the archives and add all environment variables:
https://archive.apache.org/dist/spark/

Watch the videos below for a complete walkthrough of how
to install spark. The videos follow Windows machine but the equivalent applies for Linux:

Prerequisites: https://www.youtube.com/watch?v=cRxXXqb_zmo&list=PLkz1SCf5iB4f279Cnt2SUNtJ0NdZZoFnt&index=4

Installing spark: https://www.youtube.com/watch?v=bdEYDo3Icaw&list=PLkz1SCf5iB4f279Cnt2SUNtJ0NdZZoFnt&index=5

Donwload the latest Apache Kafka release from here. Save it in the project directory and configure the kafka environment variable in the .bashrc file:
NOTE: For this project, the kafka environment variable is called KAFKA_PYSPARK_ANALYTICS_PROJECT_HOME.

https://kafka.apache.org/downloads


## Kafka Config Setup
Go to the kafka installation, go to config and then make the following changes: 

* Open the server.properties file. Set the "log.dirs" variable to /tmp/kafka-logs.
* In the server.properties file, set the "advertised.listeners" variable to PLAINTEXT://localhost:9092
* Open the zookeeper.properties file. Set the "dataDir" variable to /tmp/zookeeper

Save the files and exit.

Open the project in an IDE and move to 'kafka-scripts' folder in the terminal:

```
cd kafka-scripts
```

Run the 3 following commands in 3 separate intervals. These are the zookeeper start script, kafka server start script and the topic creation script:

```
sh zookeeper-start.sh
sh kafka-start.sh
sh create-transaction-topic.sh
```

### The kafka single node setup is now up.

### Python environment

Open a new terminal and create a virtual environment using the following command:
```
python3 -m venv venv
```
Activate the environment
```
source venv/bin/activate
```

Install all packages using the requirements file

```
pip install -r requirements.txt
```

### Run the spark streaming app python file:

```
python spark-stream-app,py
```
The application logs information onto a log.txt file which is available in the application_logs directory.
The dataframes output will be shown on the console by default. If you want to use a file sink, then set it appropriately using the config file.
NOTE: File Sink only works in append output mode.

### Kafka Producer

Finally, send some messages to the kafka topic using the transaction producer file:

```
python transactions-producer.py
```

#### NOTE: The "lib" directory contains two files: 

* utils.py: All helper functions for spark code, kafka producer code and configuration settings.
* logger.py: Contains the custom Logger class for documenting runs. Outputs a log file to the application logs folder.

#### Default configurations for spark streaming code and kafka producer files can be changed in the "config.yaml" file. 

## Future improvments

* Extending the application to build a full end-to-end use analytics case, using some databases like Cassandra / PostgreSQL.
* Adding scheduling capabilities using Apache Airflow.
* Adding dashboarding capabilities -- Power BI, Data Studio etc

