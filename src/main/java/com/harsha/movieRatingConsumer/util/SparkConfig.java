package com.harsha.movieRatingConsumer.util;

import org.apache.spark.SparkConf;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfig {

    @Bean
    public SparkConf sparkConf() {
        return new SparkConf()
                .setAppName("movieRatingConsumer")
                .setMaster("local[*]")
                .set("spark.executor.memory","1g")
                .set("spark.driver.allowMultipleContexts", "true");
    }
}