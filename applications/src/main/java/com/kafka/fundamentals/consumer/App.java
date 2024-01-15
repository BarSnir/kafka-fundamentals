package com.kafka.fundamentals.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {
    private static final Logger logger =  LoggerFactory.getLogger(
        App.class.getSimpleName()
    );

    public static void main(String[] args) {
        String topic = "HelloWorldTopic";
        KafkaConsumer<String, String> consumer = getConsumer();
        Thread mainThread = Thread.currentThread();
        addShutdownHook(mainThread, consumer);
        // subscribe && poll
        try {
            consumer.subscribe(Arrays.asList(topic));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));  
                for (ConsumerRecord<String, String> record: records) {
                    logger.info(
                        "Record with key" + record.key() + " has value of " + record.value()
                    );
                    logger.info(
                        "The Record comes from partition" + record.partition() + " with offset of " + record.offset()
                    );
                }
            }
        } catch (WakeupException e) {
            logger.error("Wakeup exception accrued! " + e.getMessage());
        } catch (Exception e) {
              logger.error("Unknown exception: " + e.getMessage());
        } finally {
              consumer.close();
              logger.info("The consumer shutdown gracefully!");
        }
    }

    public static KafkaConsumer<String, String> getConsumer() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "HelloWorldCG");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());
        props.setProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "HelloWorldCG-A");
        return new KafkaConsumer<>(props);
    }

    public static void addShutdownHook(Thread main, KafkaConsumer<String, String> consumer) {
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run() {
                logger.warn("Detect custom shutdown, triggering Wakeup shutdown");
                consumer.wakeup();
                try {
                    main.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }
}
