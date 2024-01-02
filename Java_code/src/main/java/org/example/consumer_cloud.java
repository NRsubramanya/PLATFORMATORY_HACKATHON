package org.example;


import io.confluent.connect.replicator.offsets.ConsumerTimestampsInterceptor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;


import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;


public class consumer_cloud {


    public static void main(String[] args) throws InterruptedException {


        Properties prty = new Properties();

        try {
            FileInputStream input = new FileInputStream("/home/sumo/hack_task1/cloud_client.properties");
            prty.load(input);
            input.close();
        }catch (IOException e) {
            e.printStackTrace();
            return;
        }

        //prty.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        prty.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "pkc-4r087.us-west2.gcp.confluent.cloud:9092");
        prty.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prty.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prty.setProperty(ConsumerConfig.GROUP_ID_CONFIG , "ConsumerGroup3");
        prty.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        System.out.println(prty.toString());
        prty.setProperty(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, ConsumerTimestampsInterceptor.class.getName());
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(prty);



        consumer.subscribe(Arrays.asList("transfer1_topic"));

        while (true) {
            ConsumerRecords<String, String> records =  consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord record: records) {
                System.out.println("Received new record: \n" +
                        "Key: " + record.key() + ", " +
                        "Value: " + record.value() + ", " +
                        "Topic: " + record.topic() + ", " +
                        "Offset: " + record.offset() + ", " +
                        "Partition: " + record.partition() + "\n");
                Thread.sleep(1000 * 20);
                consumer.commitSync();
            }

        }
    }

}