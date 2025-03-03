package com.learnkafka.jpa;

import com.learnkafka.entity.FailureRecord;
import org.springframework.data.repository.CrudRepository;

import java.util.List;

// for query creation "https://docs.spring.io/spring-data/jpa/reference/jpa/query-methods.html"
public interface FailureRecordRepository extends CrudRepository<FailureRecord,Integer> {
    List<FailureRecord> findAllByStatus(String retry);
}
