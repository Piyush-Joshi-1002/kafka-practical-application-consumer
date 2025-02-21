package com.learnkafka.jpa;

import com.learnkafka.entity.Book;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;
import java.util.Optional;

@Repository
public interface BookRepository extends CrudRepository<Book, Long> {
    boolean existsByLibraryEventId(Long libraryEventId);
    Optional<Book> findByLibraryEventId(Long libraryEventId);
}
