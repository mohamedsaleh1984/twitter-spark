
### Twitter-Kafka-Spark-HBase Integration

Fetch a stream of tweets from twitter, push it to Kafka, ingest the stream into Spark Stream and save it to Hive  

# Getting Started

### 1. Kakfa Installation 

1. Download and extract Kafka to your machine from https://downloads.apache.org/kafka/3.2.1/kafka_2.13-3.2.1.tgz
2. Extra it and locate it to your home directory, added the bin location to your system environment.

### 2. Runing Kafka
1. run *.sh on linux Or *.bat on windows to make sure that no existsing kafka
2. run ZooKeeper, after you run zookeeper you should get.  
   > **./zookeeper-server-start.sh ../config/zookeeper.properties**
 
   >> **binding to port 0.0.0.0/0.0.0.0:2181**
   
   
3. run Kafka Server, after you run kafka-server you should get. 
   > **./kafka-server-start.sh ../config/server.properties**
   
   >> **Started socket server acceptors and processors**

### 3. Create Kafka Topic
Creating kafka topic where we receive our stream. 
   > **./kafka-topics.sh --create --topic=twitter-topic-new --bootstrap-server localhost:9092 --replication-factor=1 --partitions=1**

   >> Created topic twitter-topic-new.
   
To list current topics (Testing)
   > **./kafka-topics.sh --list --bootstrap-server localhost:9092**

## 4. Test Kafka Consumer (Testing)
> **./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic twitter-topic-new --from-beginning**

### Tools
Kafka **[Kafka](https://kafka.apache.org/quickstart)**

### References

Kafka Documentation **[Kafka Documentation](https://kafka.apache.org/documentation/#quickstart)**
