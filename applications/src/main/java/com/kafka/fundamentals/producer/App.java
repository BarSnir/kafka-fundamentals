package com.kafka.fundamentals.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {
    private static final Logger logger =  LoggerFactory.getLogger(
        App.class.getSimpleName()
    );
    public static void main(String[] args){
        KafkaProducer<String, String> producer = getProducer();
        for (int j = 0; j < 10; j++ ){
            try {            
                for (int i = 0; i < 30; i++) {
                String topic = "HelloWorldTopic";
                String key = String.valueOf(i);
                String value = "Current value: " + String.valueOf(i);
                ProducerRecord<String, String> record = new ProducerRecord(
                    topic, key, value
                );
                produce(producer, record);
            }
                Thread.sleep(1500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        producer.close();
    }

    public static KafkaProducer<String, String> getProducer() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "400");
        return new KafkaProducer<>(props);
    }

    public static void produce(KafkaProducer<String, String> producer, ProducerRecord<String, String> record){
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception e) {
                if(e == null) {
                    logger.info(
                        "onCompletion Status: \n"+ 
                        "Topic:" + metadata.topic() + 
                        " Partition:" + metadata.partition() + 
                        " Key: " + record.key() +
                        " Offset:" +metadata.offset() +
                        " Timestamp:" + metadata.timestamp()
                    );
                }
            }
        });
        // sync ops
        producer.flush();
    }
}
