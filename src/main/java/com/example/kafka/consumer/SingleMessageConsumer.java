package com.example.kafka.consumer;

import com.example.kafka.message.Message;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class SingleMessageConsumer implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(SingleMessageConsumer.class);
    private final KafkaConsumer<String, String> consumer;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public SingleMessageConsumer() {
        String bs = System.getenv().getOrDefault(
                "KAFKA_BOOTSTRAP_SERVERS",
                "kafka-1:9092,kafka-2:9092,kafka-3:9092"
        );

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bs);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "single-group"); // Уникальная группа
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true"); // Автокоммит
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Начинать с начала
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1"); // Только 1 сообщение за poll

        this.consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("my_topic"));
    }

    @Override
    public void run() {
        try {
            while (true) {
                var records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        Message msg = objectMapper.readValue(record.value(), Message.class); // Десериализация с Jackson
                        System.out.println("Single Consumer consumed: " + msg); // Вывод на консоль
                    } catch (Exception e) {
                        System.out.println("Deserialization error in Single Consumer: " + e.getMessage()); // Обработка ошибки
                        logger.error("Deserialization error", e);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error in single consumer", e); // Логируем и продолжаем (цикл вечный)
        } finally {
            consumer.close();
        }
    }
}
