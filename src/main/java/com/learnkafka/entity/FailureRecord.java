package com.learnkafka.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Entity
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class FailureRecord {
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Integer id;
    private String topic;
    private Integer kafka_key;
    private String errorRecord;
    private Integer partition;
    private Long offset_value;
    private String exception;
    private String status;

}
