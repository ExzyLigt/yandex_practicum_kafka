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

public class BatchMessageConsumer implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(BatchMessageConsumer.class);
    private final KafkaConsumer<String, String> consumer;
    private final ObjectMapper objectMapper = new ObjectMapper(); // Jackson ObjectMapper

    public BatchMessageConsumer() {
        String bs = System.getenv().getOrDefault(
                "KAFKA_BOOTSTRAP_SERVERS",
                "kafka-1:9092,kafka-2:9092,kafka-3:9092"
        );

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bs);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "batch-group"); // Уникальная группа
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // Ручной коммит
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "10240"); // Мин. 10KB (примерно для 10 сообщений, если каждое ~1KB)
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "5000"); // Макс. ожидание 5 сек для накопления мин. байт
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100"); // Макс. за poll, но мы ждем мин. 10

        this.consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("my_topic"));
    }

    @Override
    public void run() {
        try {
            while (true) {
                var records = consumer.poll(Duration.ofMillis(5000)); // Poll с таймаутом
                if (records.count() >= 10) { // Обрабатываем только если >=10
                    for (ConsumerRecord<String, String> record : records) {
                        try {
                            Message msg = objectMapper.readValue(record.value(), Message.class); // Десериализация с Jackson
                            System.out.println("Batch Consumer consumed: " + msg); // Вывод на консоль
                        } catch (Exception e) {
                            System.out.println("Deserialization error in Batch Consumer: " + e.getMessage()); // Обработка ошибки
                            logger.error("Deserialization error", e);
                        }
                    }
                    consumer.commitSync(); // Ручной коммит после пачки
                } else if (!records.isEmpty()) {
                    logger.info("Received less than 10 messages, skipping commit");
                }
            }
        } catch (Exception e) {
            logger.error("Error in batch consumer", e); // Логируем и продолжаем
        } finally {
            consumer.close();
        }
    }
}