package com.learnkafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.entity.Book;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.jpa.BookRepository;
import com.learnkafka.jpa.LibraryEventsRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Slf4j
@RequiredArgsConstructor
public class LibraryEventsService {

    private final ObjectMapper objectMapper;
    private final LibraryEventsRepository libraryEventsRepository;
    private final BookRepository bookRepository;

    @Transactional
    public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
        log.info("Processing Library Event: {}", libraryEvent);

        if (libraryEvent.getLibraryEventType() == null) {
            log.error("Invalid Library Event Type");
            return;
        }

        switch (libraryEvent.getLibraryEventType()) {
            case NEW -> save(libraryEvent);
            case UPDATE -> update(libraryEvent);
            default -> log.error("Invalid Library Event Type: {}", libraryEvent.getLibraryEventType());
        }
    }

    private void save(LibraryEvent libraryEvent) {
        if (libraryEvent.getBook() == null) {
            log.error("Book information is missing in the event");
            throw new IllegalArgumentException("Book cannot be null for a NEW event");
        }
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventsRepository.save(libraryEvent);
        log.info("Successfully persisted Library Event: {}", libraryEvent);
    }

    private void update(LibraryEvent libraryEvent) {
        if (libraryEvent.getLibraryEventId() == null) {
            throw new IllegalArgumentException("Library Event ID is null for update");
        }

        LibraryEvent existingEvent = libraryEventsRepository.findById(libraryEvent.getLibraryEventId())
                .orElseThrow(() -> new IllegalArgumentException("Library Event ID " + libraryEvent.getLibraryEventId() + " not found for update"));

        // Ensure the book exists before updating
        Book existingBook = bookRepository.findByLibraryEventId(Long.valueOf(libraryEvent.getLibraryEventId()))
                .orElseThrow(() -> new IllegalArgumentException("Book not found for Library Event ID: " + libraryEvent.getLibraryEventId()));

        save(libraryEvent);
        log.info("Successfully updated Library Event: {}", existingEvent);
    }
}
