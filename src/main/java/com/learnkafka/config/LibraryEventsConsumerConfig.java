package com.learnkafka.config;


import com.learnkafka.service.FailureService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.Field;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.*;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.FixedBackOff;

import java.util.List;

@Configuration
@EnableKafka
@Slf4j
public class LibraryEventsConsumerConfig {

    public static  final String RETRY = "RETRY";
    public static final String DEAD = "DEAD";
    @Value("${topics.retry}")
    private String retryTopic;

    @Value("${topics.dlt}")
    private String daedLetterTopic;

    @Autowired
    FailureService failureService;

    /*  *___if we have mutliple Recoverable exceptions ___*
    ________________________________________________________________________________________________
    *   private static final Set<Class<?>> RETRYABLE_ERRORS = Set.of(
            RecoverableDataAccessException.class,
            SomeOtherRecoverableException.class,
            AnotherRecoverableException.class
        );

        public DeadLetterPublishingRecoverer publishingRecoverer() {
            return new DeadLetterPublishingRecoverer(template, (r, e) -> {
                log.error("Exception in publishingRecoverer : {}", e.getMessage(), e);
                return RETRYABLE_ERRORS.contains(e.getCause().getClass())
                    ? new TopicPartition(retryTopic, r.partition())
                    : new TopicPartition(deadLetterTopic, r.partition());
            });
        }
    * _____________________________________________________________________________________________
    * */


    @Autowired
    KafkaTemplate template;
    // we can also use DefaultAfterRollbackProcessor with a record recover.
    public DeadLetterPublishingRecoverer publishingRecoverer(){
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template,
                (r, e) -> {
                    log.error("Exception in publishingRecoverer : {}", e.getMessage(), e);
                    if (e.getCause() instanceof RecoverableDataAccessException) {
                        return new TopicPartition(retryTopic, r.partition());
                    }
                    else {
                        return new TopicPartition(daedLetterTopic, r.partition());
                    }
                });
        return  recoverer;

    }

    ConsumerRecordRecoverer consumerRecordRecoverer = (consumerRecord, e) -> {
        log.error("Exception in consumerRecordRecoverer : {}, and message {}", consumerRecord ,e.getMessage(), e);
        var record = (ConsumerRecord<Integer,String>) consumerRecord;
        Integer key = record.key() == null ?0:record.key();
        if (e.getCause() instanceof RecoverableDataAccessException) {
            //recovery logic
            log.info("Inside consumer recovery ");
            failureService.saveFailedRecord(record,e,RETRY);
        }
        else {
            //non-recovery logic
            log.info("Inside  consumer non-recovery ");
            failureService.saveFailedRecord(record,e,DEAD);
        }
    };
    private final KafkaProperties kafkaProperties;

    public LibraryEventsConsumerConfig(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    public DefaultErrorHandler errorHandler(){

        // FixedBackOff: 1-second delay, max 2 retries
        FixedBackOff fixedBackOff = new FixedBackOff(1000L, 2); //used for retry attempt and fixed duration between each attempt

        // ExponentialBackOffWithMaxRetries: max 2 retries, initial delay 1 second
        var expBackOff = new ExponentialBackOffWithMaxRetries(2); // used for max retry attempt and duration between each attempt is exponential
        expBackOff.setInitialInterval(1000L); // 1-second initial delay
        expBackOff.setMultiplier(2.0); // Exponential growth factor
        //expBackOff.setMaxElapsedTime(2000L); // Max total wait time (It's not working )


        // Error handeler
        var errorHandler =  new DefaultErrorHandler(
//              publishingRecoverer(), /* not using for database storage
                consumerRecordRecoverer,
//              fixedBackOff
                expBackOff
        );

        /* exceptions where we don't want our kafka to retry on */
        var exceptionsToIgnoreList = List.of(
                IllegalArgumentException.class
        );

        exceptionsToIgnoreList.forEach(errorHandler::addNotRetryableExceptions);
        /*_________End________*/

        /*Checking and listening what's happening inside retry */
        errorHandler
                .setRetryListeners(((record, ex, deliveryAttempt) -> {
                    log.info("Failed Record in Retry Listener Exception : {} , deliveryAttempt : {}",
                            ex.getMessage(),deliveryAttempt);
                }));

        /*________End_________*/




        return errorHandler;
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ObjectProvider<ConsumerFactory<Object, Object>> kafkaConsumerFactory,
            ObjectProvider<ContainerCustomizer<Object, Object, ConcurrentMessageListenerContainer<Object, Object>>> kafkaContainerCustomizer,
            ObjectProvider<SslBundles> sslBundles) {

        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory.getIfAvailable(() ->
                new DefaultKafkaConsumerFactory<>(kafkaProperties.buildConsumerProperties())));

        //*__for set multiple kafka listeners from same application itself__* ,
        // it's recommended if your application is not running in cloud like environment

        factory.setConcurrency(3);
        factory.setCommonErrorHandler(errorHandler());

        // *__Set acknowledgment mode manual__*
        //factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        //factory.getContainerProperties().setAckCount(100); // Commit after 100 records

        kafkaContainerCustomizer.ifAvailable(factory::setContainerCustomizer);
        return factory;
    }
}


