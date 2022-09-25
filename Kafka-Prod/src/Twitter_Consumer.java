import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;


public class Twitter_Consumer extends Thread{
	private final String BOOTSTRAP_SERVER = "localhost:9092";
	private final String KAFKA_TOPIC = "twitter-topic-new";
	
	public void run(){
		System.out.print("Kafka Consumer Thread Started...");
		
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
		props.put("group.id", "test");
		props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");  
		props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
		
		//Declare/Define Kafka Consumer...
		KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(props);
		//Subscribe
		consumer.subscribe(Arrays.asList(KAFKA_TOPIC));
		
		//Start Reading Data from Kafka
		while(true){
			//fetch every 100ms
			ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
			
			for(ConsumerRecord<String, String> record:records){
				System.out.println("****KEY" +  record.key() +"  ****");
			}
			//Value " + record.value() +"
		}
		
		
		
	}
}