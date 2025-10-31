package com.example.kafka;

import com.example.kafka.consumer.BatchMessageConsumer;
import com.example.kafka.consumer.SingleMessageConsumer;
import com.example.kafka.producer.Producer;


import java.lang.Thread;


public class KafkaApp {
    public static void main(String[] args) {
        // Запускаем продюсер в отдельном потоке: он будет отправлять сообщения асинхронно (push-модель)
        Thread producerThread = new Thread(new Producer());
        producerThread.start();

        // Запускаем первый консьюмер (SingleMessageConsumer) в отдельном потоке:
        // Он читает по одному сообщению, с автокоммитом
        Thread singleConsumerThread = new Thread(new SingleMessageConsumer());
        singleConsumerThread.start();

        // Запускаем второй консьюмер (BatchMessageConsumer) в отдельном потоке:
        // Он читает пачками (минимум 10 сообщений), с ручным коммитом после обработки
        Thread batchConsumerThread = new Thread(new BatchMessageConsumer());
        batchConsumerThread.start();

        // Ждём завершения продюсера (он отправит все сообщения и завершится).
        try {
            producerThread.join(); // Ожидаем завершения продюсера
            // Консьюмеры продолжают работать параллельно и независимо (в разных группах, получая все сообщения)
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // Обработка прерывания
        }
    }
}