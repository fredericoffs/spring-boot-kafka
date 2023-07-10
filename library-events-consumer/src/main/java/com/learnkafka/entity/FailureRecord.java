package com.learnkafka.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class FailureRecord {

    @Id
    @GeneratedValue
    private Integer id;
    private String topic;
    private Integer keyValue;
    private String errorRecord;
    private Integer partition;
    private Long offsetValue;
    private String exception;
    private String status;

}
