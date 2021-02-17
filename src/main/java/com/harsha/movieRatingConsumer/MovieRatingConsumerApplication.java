package com.harsha.movieRatingConsumer;

import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class MovieRatingConsumerApplication {

	public static JavaSparkContext sparkContext;

	private final SparkConsumerService sparkConsumerService;

	public MovieRatingConsumerApplication(SparkConsumerService sparkConsumerService) {
		this.sparkConsumerService = sparkConsumerService;
	}

//	public void run(String[] args) throws Exception {
//		sparkConsumerService.run();
//	}

	public static void main(String[] args) {
		SpringApplication.run(MovieRatingConsumerApplication.class, args);

	}

}
