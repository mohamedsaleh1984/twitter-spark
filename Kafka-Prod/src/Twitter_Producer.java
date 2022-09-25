import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.google.gson.Gson;
import com.twitter.clientlib.ApiException;
import com.twitter.clientlib.TwitterCredentialsBearer;
import com.twitter.clientlib.api.TwitterApi;

public class Twitter_Producer extends Thread {
	private final String KAFKA_TOPIC = "twitter-topic-new";
	private final String BOOTSTRAP_SERVER = "localhost:9092";
	private final String BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAACa1gwEAAAAAM4MnSIDVY1PA9MI0%2FHW7yAPPyZk%3DsIS0VrIZ59iPPNXVRWcJwCm8uwtgWlDvqaKM7hmxVlHml7DJ4j";

	public void run() {
		System.out.println("Twitter Producer Thread Started...");
		
		TwitterCredentialsBearer credentials = new TwitterCredentialsBearer(
				BEARER_TOKEN);
		TwitterApi apiInstance = new TwitterApi(credentials);

		Set<String> tweetFields = new HashSet<>();
		tweetFields.add("created_at");
		tweetFields.add("referenced_tweets");
		tweetFields.add("lang");

		Set<String> expansions = new HashSet<>();
		expansions.add("author_id");

		Set<String> place = new HashSet<String>();
		place.add("country");

		Set<String> userFields = new HashSet<>();
		userFields.add("location");

		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVER);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class.getName());
	 
		
		KafkaProducer<String, String> kafka_twitter_prod = new KafkaProducer<>(
				props);
		

		try {

			InputStream result = apiInstance.tweets().searchStream()
					.tweetFields(tweetFields).userFields(userFields)
					.expansions(expansions).placeFields(place).execute();

			try {
				BufferedReader reader = new BufferedReader(
						new InputStreamReader(result));
				String line = reader.readLine();
				while (line != null) {
					// Skip empty lines
					if (line.isEmpty()) {
						line = reader.readLine();
						continue;
					}

					// Convert line into Json Object
					Root tweetObject = new Gson().fromJson(line, Root.class);
					System.out.println(line);
					
					//Conveeert to Tweet Model
					TweetModel tm = new TweetModel();
					tm.id = tweetObject.data.id;
					tm.text = tweetObject.data.text;
					tm.lang = tweetObject.data.lang;
					
				
					// Push it to Kafka Producer
					ProducerRecord<String, String> rec = new ProducerRecord<String, String>(KAFKA_TOPIC, tweetObject.data.id, new Gson().toJson(tm));
						
					kafka_twitter_prod.send(rec);
						
					kafka_twitter_prod.flush();

					line = reader.readLine();
				}
				
				kafka_twitter_prod.close();

			} catch (Exception e) {
				e.printStackTrace();
				System.out.println(e);
			}
		} catch (ApiException e) {
			System.err.println("Exception when calling TweetsApi#searchStream");
			System.err.println("Status code: " + e.getCode());
			System.err.println("Reason: " + e.getResponseBody());
			System.err.println("Response headers: " + e.getResponseHeaders());
			e.printStackTrace();
		}
	}
}