package com.harsha.movieRatingConsumer;

import java.util.Arrays;
import java.util.Collection;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.harsha.movieRatingConsumer.util.KafkaConsumerConfig;

import scala.Tuple2;

@Service
public class SparkConsumerService {

//	@Autowired
//	SparkConf sparkConf;
	
	private final Logger log = LoggerFactory.getLogger(SparkConsumerService.class);

	private final SparkConf sparkConf;

	private final KafkaConsumerConfig kafkaConsumerConfig;

	private final KafkaProperties kafkaProperties;

	private final Collection<String> topics;

	public SparkConsumerService(SparkConf sparkConf, KafkaConsumerConfig kafkaConsumerConfig,
			KafkaProperties kafkaProperties) {
		this.sparkConf = sparkConf;
		this.kafkaConsumerConfig = kafkaConsumerConfig;
		this.kafkaProperties = kafkaProperties;
		this.topics = Arrays.asList(kafkaProperties.getTemplate().getDefaultTopic());
	}

//    @KafkaListener(topics = "spark_kafka_poc")
//    public void consume(String message) {
//        System.out.println("Consumed message: " + message);
//    }
    
	@KafkaListener(topics = "spark_kafka_poc")
	public void run() {
		log.debug("Running Spark Consumer Service..");

		// Create context with a 10 seconds batch interval
		JavaStreamingContext jssc = new JavaStreamingContext(new JavaSparkContext(sparkConf), Durations.seconds(10));

		// Create direct kafka stream with brokers and topics
		JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(jssc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.Subscribe(topics, kafkaConsumerConfig.consumerConfigs()));


		
		// Get the lines, split them into words, count the words and print
		// Count the tweets and print
	    // Get the lines, split them into words, count the words and print
	    JavaDStream<String> lines = messages.map(ConsumerRecord::value);
	    JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(Pattern.compile(" ").split(x)).iterator());
	    JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
	        .reduceByKey((i1, i2) -> i1 + i2);
	    wordCounts.print();
		

		// Start the computation
		jssc.start();
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			log.error("Interrupted: {}", e);
			// Restore interrupted state...
		}
	}
}
