package com.integralads.crawler.kafka;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class TestProducer {
    
    public static void main(String[] args) throws Exception {
        
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", "0");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        
        Producer<String, String> producer = new KafkaProducer<>(props);
        
        String line;
        String file = args[0];
        int partition = 0;
        int i = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            while ((line = br.readLine()) != null) {
                i++;
                partition = (partition + 1) % 3;
                producer.send(new ProducerRecord<String, String>("test", partition, Integer.toString(i), line));
                
            }
        }
        producer.close();        
    }
    
}