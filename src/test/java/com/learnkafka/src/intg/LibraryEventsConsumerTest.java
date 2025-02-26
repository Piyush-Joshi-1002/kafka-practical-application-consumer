package com.learnkafka.src.intg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.consumer.LibraryEventsConsumer;
import com.learnkafka.entity.Book;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.entity.LibraryEventType;
import com.learnkafka.jpa.LibraryEventsRepository;
import com.learnkafka.service.LibraryEventsService;

@SpringBootTest
@EmbeddedKafka(topics = {"library-events","library-events.RETRY","library-events.DLT"}, partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}"
        , "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}" })
class LibraryEventsConsumerTest {
    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    KafkaTemplate<Integer,String> kafkaTemplate;

    @Autowired
    KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @MockitoSpyBean
    LibraryEventsConsumer libraryEventsConsumerSpy;

    @MockitoSpyBean
    LibraryEventsService libraryEventsServiceSpy;

    @Value("${topics.retry}")
    private String retryTopic;

    @Value("topics.dlt")
    private String daedLetterTopic;

    @Autowired
    LibraryEventsRepository libraryEventsRepository;

    private Consumer<Integer,String> consumer;


    @Autowired
    ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        for(MessageListenerContainer messageListenerContainer : kafkaListenerEndpointRegistry.getAllListenerContainers()){
            ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());

        }
    }

    @AfterEach
    void tearDown(){
        libraryEventsRepository.deleteAll();
    }


    @Test
    void publishNewLibraryEventTest() throws ExecutionException, InterruptedException, JsonProcessingException {

        //given
        String json = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":457,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"XYZ\"}}";
        kafkaTemplate.sendDefault(json).get();

        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        // then
        verify(libraryEventsConsumerSpy,times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy,times(1)).processLibraryEvent(isA(ConsumerRecord.class));

        libraryEventsRepository.findAll();
        List<LibraryEvent> libraryEvents = (List<LibraryEvent>) libraryEventsRepository.findAll();
        assert libraryEvents.size() == 1;
        libraryEvents.forEach(libraryEvent -> {
            assert libraryEvent.getLibraryEventId()!=null;
            assertEquals(457,libraryEvent.getBook().getBookId());
        });
    }

    @Test
    void publishUpdateLibraryEventTest() throws ExecutionException, InterruptedException, JsonProcessingException {
        // given
        String json = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":457,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"XYZ\"}}";

        // save the new LibrarayEvent
        LibraryEvent libraryEvent = objectMapper.readValue(json,LibraryEvent.class);
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventsRepository.save(libraryEvent);

        // publish the update LibraryEvent
        Book updatedBook = Book.builder().bookId(457).bookName("Kafka Using spring boot 2.x.1")
                .bookAuthor("ABC").build();
        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        libraryEvent.setBook(updatedBook);
        String updatedJson = objectMapper.writeValueAsString(libraryEvent);

        kafkaTemplate.sendDefault(libraryEvent.getLibraryEventId(),updatedJson).get();


        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        // then
        verify(libraryEventsConsumerSpy,times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy,times(1)).processLibraryEvent(isA(ConsumerRecord.class));

        LibraryEvent pesrsistedLibraryEvent = libraryEventsRepository.findById(libraryEvent.getLibraryEventId()).get();
        assertEquals("Kafka Using spring boot 2.x.1",pesrsistedLibraryEvent.getBook().getBookName());

    }
    @Test
    void publishUpdateLibraryEvent_null_LibraryEvent_Test() throws ExecutionException, InterruptedException, JsonProcessingException {
        // given
        String json = "{\"libraryEventId\":null,\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":457,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"XYZ\"}}";

        kafkaTemplate.sendDefault(json).get();


        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(5, TimeUnit.SECONDS);

        // then
        // Before
        // Expected three invocations before making IllegalArgumentException non-retriable in the config file
//        verify(libraryEventsConsumerSpy, times(3)).onMessage(isA(ConsumerRecord.class));
//        verify(libraryEventsServiceSpy, times(3)).processLibraryEvent(isA(ConsumerRecord.class));

        // After
        // Changes after making IllegalArgumentException non-retriable in the config file
        verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));


    }

    @Test
    void publishUpdateLibraryEvent_999_LibraryEvent_Test() throws ExecutionException, InterruptedException, JsonProcessingException {
        // given
        String json = "{\"libraryEventId\":999,\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":457,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"XYZ\"}}";

        kafkaTemplate.sendDefault(json).get();


        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(5, TimeUnit.SECONDS);

        //then
        verify(libraryEventsConsumerSpy, times(3)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(3)).processLibraryEvent(isA(ConsumerRecord.class));

        Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("group1","true",embeddedKafkaBroker));

        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");
        consumer = new DefaultKafkaConsumerFactory<>(configs,new IntegerDeserializer(),new StringDeserializer()).createConsumer();
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer,retryTopic);

        ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer,retryTopic);
        System.out.println("consumerRecord is : " + consumerRecord.value());
        assertEquals(json,consumerRecord.value());

    }
}