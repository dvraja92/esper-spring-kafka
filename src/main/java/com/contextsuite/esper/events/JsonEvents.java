package com.contextsuite.esper.events;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class JsonEvents {

    private final String[] topics = {"A", "B"};
    private int currentTopicIndex = 0;

    private final KafkaTemplate<String, String> kafkaTemplate;
    private Integer age = 20;
    @Scheduled(fixedRate = 100000)
    public void produceJsonEvent() throws JsonProcessingException {
        String currentTopic = topics[currentTopicIndex];
        String personEvent = generatePersonEvent(currentTopic);
        kafkaTemplate.send(currentTopic, personEvent);
        log.info(currentTopic, personEvent);
        currentTopicIndex = (currentTopicIndex + 1) % topics.length;
    }
    private String generatePersonEvent(String currentTopic) throws JsonProcessingException {
        EsperWrappedPersonEvent personEvent = new EsperWrappedPersonEvent();
        personEvent.setAge(age);
        personEvent.setName("Name: "+age);
        personEvent.setSource(currentTopic);
        age = age + 1;
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString(personEvent);
    }

}
