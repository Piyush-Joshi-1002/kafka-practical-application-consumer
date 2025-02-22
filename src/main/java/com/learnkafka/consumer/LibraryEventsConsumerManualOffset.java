package com.learnkafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

//@Component
@Slf4j
public class LibraryEventsConsumerManualOffset implements AcknowledgingMessageListener<Integer,String> {

//    @KafkaListener(topics = {"library-events"})
////  @KafkaListener(topics = {"${spring.kafka.topic}"})
//    public void onMessage(ConsumerRecord<Integer,String> consumerRecord){
//        log.info("consumer Record : {}", consumerRecord);
//
//    }

    @Override

    @KafkaListener(topics = {"library-events"})
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord, Acknowledgment acknowledgment) {
        log.info("consumer Record : {}", consumerRecord);
        acknowledgment.acknowledge();
    }
}
