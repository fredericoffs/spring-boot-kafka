package com.learnkafka.config;

import com.learnkafka.service.FailureService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.FixedBackOff;

import java.util.List;

@Configuration
@EnableKafka
@Slf4j
public class LibraryEventsConsumerConfig {

    public static final String RETRY = "RETRY";
    public static final String DEAD = "DEAD";

    @Autowired
    KafkaTemplate kafkaTemplate;

    @Autowired
    FailureService failureService;

    @Value("${topics.retry}")
    private String retryTopic;

    @Value("${topics.dlt}")
    private String deadLetterTopic;

    private final KafkaProperties properties;

    public LibraryEventsConsumerConfig(KafkaProperties properties) {
        this.properties = properties;
    }

    public DeadLetterPublishingRecoverer publishingRecoverer() {
        return new DeadLetterPublishingRecoverer(kafkaTemplate,
                (r, e) -> {
                    log.error("Exception in publishingRecoverer: {}", e.getMessage(), e);
                    if (e.getCause() instanceof RecoverableDataAccessException) {
                        return new TopicPartition(retryTopic, r.partition());
                    } else {
                        return new TopicPartition(deadLetterTopic, r.partition());
                    }
                });
    }
//    CommonErrorHandler errorHandler = new DefaultErrorHandler(recoverer, new FixedBackOff(0L, 2L));

    ConsumerRecordRecoverer consumerRecordRecoverer = (consumerRecord, e) -> {
        log.error("Exception in publishingRecoverer: {}", e.getMessage(), e);
        var record = (ConsumerRecord<Integer, String>) consumerRecord;
        if (e.getCause() instanceof RecoverableDataAccessException) {
            //recovery logic
            log.info(("Inside recovery"));
            failureService.saveFailedRecord(record, e, RETRY);
        } else {
            //non-recovery logic
            log.info(("Inside non-recovery"));
            failureService.saveFailedRecord(record, e, DEAD);
        }
    };

    public DefaultErrorHandler errorHandler() {
        var exceptionToIgnoreList = List.of(
                IllegalArgumentException.class
        );

//        var exceptionToRetryList = List.of(
//                RecoverableDataAccessException.class
//        );

        var fixedBackOff = new FixedBackOff(1000L, 2);

        var expBackOff = new ExponentialBackOffWithMaxRetries(2);
        expBackOff.setInitialInterval(1_000L);
        expBackOff.setMultiplier(2.0);
        expBackOff.setMaxInterval(2_000);

        var errorHandler = new DefaultErrorHandler(
//                publishingRecoverer(),
                consumerRecordRecoverer,
                fixedBackOff
//                expBackOff
        );

        exceptionToIgnoreList.forEach(errorHandler::addNotRetryableExceptions);
//        exceptionToRetryList.forEach(errorHandler::addRetryableExceptions);

        errorHandler
                .setRetryListeners((record, ex, deliveryAttempt) -> {
                    log.info("Failed Record in Retry Listener, Exception: {}, deliveryAttempt: {} ",
                            ex.getMessage(), deliveryAttempt);
                });

        return errorHandler;
    }

    @Bean
    @ConditionalOnMissingBean(name = "kafkaListenerContainerFactory")
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ObjectProvider<ConsumerFactory<Object, Object>> kafkaConsumerFactory,
            ObjectProvider<ContainerCustomizer<Object, Object, ConcurrentMessageListenerContainer<Object, Object>>> kafkaContainerCustomizer) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory
                .getIfAvailable(() -> new DefaultKafkaConsumerFactory<>(this.properties.buildConsumerProperties())));
        kafkaContainerCustomizer.ifAvailable(factory::setContainerCustomizer);
//        factory.setConcurrency(3);
        factory.setCommonErrorHandler(errorHandler());
//        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }
}
