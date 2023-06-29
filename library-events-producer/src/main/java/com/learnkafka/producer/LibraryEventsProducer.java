package com.learnkafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
@Slf4j
public class LibraryEventsProducer {

    @Value("${spring.kafka.topic}")
    public String topic;

    private final KafkaTemplate<Integer, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public LibraryEventsProducer(KafkaTemplate<Integer, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public CompletableFuture<SendResult<Integer, String>> sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {

        var key = libraryEvent.libraryEventId();
        var value = objectMapper.writeValueAsString(libraryEvent);

        //Asynchronous way
        //1. blocking call - get metadata about the kafka cluster
        //2. send message happens - returns a CompletableFuture

        var completableFuture = kafkaTemplate.send(topic, key, value);

        return completableFuture
                .whenComplete((sendResult, throwable) -> {
                    if (throwable != null) {
                        handleFailure(key, value, throwable);
                    } else {
                        handSuccess(key, value, sendResult);
                    }
                });
    }

    public SendResult<Integer, String> sendLibraryEvent_approach2(LibraryEvent libraryEvent)
            throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {

        var key = libraryEvent.libraryEventId();
        var value = objectMapper.writeValueAsString(libraryEvent);

        //Synchronous way
        //1. blocking call - get metadata about the kafka cluster
        //2. block and wait until the message is sent to the kafka

        var sendResult = kafkaTemplate.send(topic, key, value)
                //.get()
                .get(3, TimeUnit.SECONDS);

        handSuccess(key, value, sendResult);
        return sendResult;
    }

    public CompletableFuture<SendResult<Integer, String>> sendLibraryEvent_approach3(LibraryEvent libraryEvent) throws JsonProcessingException {

        var key = libraryEvent.libraryEventId();
        var value = objectMapper.writeValueAsString(libraryEvent);

        var producerRecord = buildProducerRecord(key, value);

        //Asynchronous way
        //1. blocking call - get metadata about the kafka cluster
        //2. send message happens - returns a CompletableFuture

        var completableFuture = kafkaTemplate.send(producerRecord);

        return completableFuture
                .whenComplete((sendResult, throwable) -> {
                    if (throwable != null) {
                        handleFailure(key, value, throwable);
                    } else {
                        handSuccess(key, value, sendResult);
                    }
                });
    }

    private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value) {
        List<Header> recordHeaders = List.of(new RecordHeader("event-source", "scanner".getBytes()));
        return new ProducerRecord<>(topic, null, key, value, recordHeaders);
    }

    private void handSuccess(Integer key, String value, SendResult<Integer, String> sendResult) {
        log.info("Message Sent Successfully for the key: {}, the value: {} and partition is {} ",
                key, value, sendResult.getRecordMetadata().partition());
    }

    private void handleFailure(Integer key, String value, Throwable throwable) {
        log.error("Error sending the message and the exception is {} ",
                throwable.getMessage(), throwable);
    }
}
