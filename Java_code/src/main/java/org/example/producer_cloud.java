package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;


public class producer_cloud{

    public static void main(String[] args) throws InterruptedException {

        Properties prty = new Properties();

        prty.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prty.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (FileInputStream input = new FileInputStream("/home/sumo/hack_task1/client.properties")) {
            prty.load(input);
        } catch (IOException e) {
            e.printStackTrace();
        }



        final KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prty);

        System.out.println("production started");

        for (int i=1;i<=20;i++){
            int partitionNumber = i % 3;
            ProducerRecord<String, String> record = new ProducerRecord<>("transfer1_topic" ,partitionNumber, "cc_key" , Integer.toString(i));
            producer.send(record);
            Thread.sleep(1000 * 10);
        }

        producer.flush();
        System.out.println("production flushed");
        producer.close();

    }
}
