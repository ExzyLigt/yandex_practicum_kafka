package com.example.kafka.producer;

import com.example.kafka.message.Message;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(Producer.class);
    private final KafkaProducer<String, String> producer;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public Producer() {
        String bs = System.getenv().getOrDefault(
                "KAFKA_BOOTSTRAP_SERVERS",
                "kafka-1:9092,kafka-2:9092,kafka-3:9092"
        );

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bs);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1"); // acks=1: At Least Once
        props.put(ProducerConfig.RETRIES_CONFIG, "5");

        this.producer = new KafkaProducer<>(props);
    }

    @Override
    public void run() {
        try {
            for (int i = 1; i <= 100; i++) { // Отправляем 100 сообщений для демонстрации batch
                Message msg = new Message(i, "Test message " + i);
                String json = objectMapper.writeValueAsString(msg); // Сериализация в JSON с Jackson
                System.out.println("Producing: " + json); // Вывод на консоль

                producer.send(new ProducerRecord<>("my_topic", json)); // Асинхронная отправка (push)
                Thread.sleep(500); // Имитация задержки
            }
        } catch (Exception e) {
            logger.error("Error in producer", e); // Логируем ошибку и продолжаем (но в этом примере цикл завершится)
        } finally {
            producer.close();
        }
    }
}