package com.learnkafka.service;

import com.learnkafka.entity.FailureRecord;
import com.learnkafka.jpa.FailureRecordRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class FailureService {
    private FailureRecordRepository failureRecordRepository;

    public FailureService(FailureRecordRepository failureRecordRepository){
        this.failureRecordRepository = failureRecordRepository;
    }


    public void saveFailedRecord(ConsumerRecord<Integer,String> consumerRecord, Exception e, String status) {
        Integer key = consumerRecord.key() == null ?0:consumerRecord.key();
        log.info("consumerRecord in  failure service file : {}",consumerRecord);
        var failureRecord = new FailureRecord(null, consumerRecord.topic(),key,
                consumerRecord.value(),consumerRecord.partition(),consumerRecord.offset(),e.getCause().getMessage(),status);
        failureRecordRepository.save(failureRecord);
    }
}
