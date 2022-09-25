package cs523.SparkConsumer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

import com.google.gson.Gson;

public class App {
	private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";

	private static Gson g = new Gson();

	public static void main(String[] args) throws Exception {

		SparkConsumer();
	}

	public static void SparkConsumer() throws ClassNotFoundException,
			SQLException {

		System.out.println("Spark Streaming Consumer started now .....");

		SparkConf conf = new SparkConf().setAppName("Spark-Consumer")
				.setMaster("local[*]");

		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaStreamingContext jssc = new JavaStreamingContext(sc, new Duration(1000));

		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "localhost:9092");
		kafkaParams.put("group.id", "id");
		kafkaParams.put("auto.offset.reset", "smallest");

		Set<String> topics = Collections.singleton("twitter-topic-new");

		JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils
				.createDirectStream(jssc, String.class, String.class,
						StringDecoder.class, StringDecoder.class, kafkaParams,
						topics);

		/***********************Hive *************************************************/
		Connection con = null;
		Class.forName("org.apache.hive.jdbc.HiveDriver");

		con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "", "");
		Statement stmt = con.createStatement();
		/******************************************************************************/
		directKafkaStream.foreachRDD(rdd -> {

			System.out.println(rdd.toString());

			List<Tuple2<String, String>> dataFor = rdd.toArray();

			for (int i = 0; i < dataFor.size(); i++) {
				Tuple2<String, String> ret = dataFor.get(i);

			 	System.out.println("**" + ret._1 + "--" + ret._2 + "**");

				TweetModel tw = new Gson().fromJson(ret._2, TweetModel.class);

				//String strSql = "INSERT INTO table tweets Values('" + tw.id + "','" + tw.text + "','" + tw.lang + "')";
				String strSql = "INSERT INTO TABLE tweets(id,tweet,lang) Values('" + tw.id + "','" + tw.text + "','" + tw.lang + "')";
				System.out.println(strSql);
				
				int c = stmt.executeUpdate(strSql);
				System.out.println("result " + c);
			}
		});

		jssc.start();
		jssc.awaitTermination();
		con.close();
	}

 		
	public void UsedLanguages() throws ClassNotFoundException, SQLException {
		Connection con = null;
		Class.forName("org.apache.hive.jdbc.HiveDriver");

		con = DriverManager.getConnection(
				"jdbc:hive2://localhost:10000/default", "", "");
		Statement stmt = con.createStatement();

		ResultSet res = stmt.executeQuery("Select Count(*), lang from tweets Groupy By lang");

		System.out.println("Result:");
		System.out.println("Count \t Language");

		while (res.next()) {
			System.out.println(res.getInt(1) + " " + res.getString(2));
		}
		con.close();
	}

}
