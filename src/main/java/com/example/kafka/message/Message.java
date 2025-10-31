package com.example.kafka.message;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Data;

@Data  // Генерирует геттеры, сеттеры, equals, hashCode, toString
@AllArgsConstructor  // Конструктор со всеми аргументами
@NoArgsConstructor // Конструктор без аргументов
public class Message {
    private int id;
    private String text;
}