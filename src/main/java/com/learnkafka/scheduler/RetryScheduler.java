package com.learnkafka.scheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.config.LibraryEventsConsumerConfig;
import com.learnkafka.entity.FailureRecord;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.jpa.FailureRecordRepository;
import com.learnkafka.service.LibraryEventsService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Slf4j
public class RetryScheduler {

    private final FailureRecordRepository failureRecordRepository;
    private final LibraryEventsService libraryEventsService;
    ObjectMapper objectMapper = new ObjectMapper();

    public RetryScheduler(FailureRecordRepository failureRecordRepository, LibraryEventsService libraryEventsService) {
        this.failureRecordRepository = failureRecordRepository;
        this.libraryEventsService = libraryEventsService;
    }

    @Scheduled(fixedDelay = 10000)
    public void retryFailedRecords(){
        log.info("Retrying Failed Records Started!");
        failureRecordRepository.findAllByStatus(LibraryEventsConsumerConfig.RETRY)
                .forEach(failureRecord -> {
                    log.info("inside for");
                    log.info("Retrying Failed Records : {}" , failureRecord);
                    try {
                        failureRecord.setStatus(LibraryEventsConsumerConfig.SUCCESS);
                        changeFailureToSuccess(failureRecord);
                        var consumerRecord = buildConsumerRecord(failureRecord);
                        libraryEventsService.processLibraryEvent(consumerRecord);


                    } catch (Exception e) {
                        log.error("Exception in retryFailedRecords : {}" , e.getMessage(), e);
                        failureRecordRepository.save(failureRecord);
                    }
                });

        log.info("Retrying Failed Records Completed!");
    }

    private void changeFailureToSuccess(FailureRecord failureRecord) throws JsonProcessingException {
        String json = failureRecord.getErrorRecord();
        LibraryEvent libraryEvent = objectMapper.readValue(json, LibraryEvent.class);
        libraryEvent.setLibraryEventId(900);
        failureRecord.setErrorRecord(objectMapper.writeValueAsString(libraryEvent));


    }

    private ConsumerRecord<Integer,String> buildConsumerRecord(FailureRecord failureRecord) {
        return new ConsumerRecord<>(
                failureRecord.getTopic(),
                failureRecord.getPartition(),
                failureRecord.getOffset_value(),
                failureRecord.getKafka_key(),
                failureRecord.getErrorRecord()
        );
    }
}
