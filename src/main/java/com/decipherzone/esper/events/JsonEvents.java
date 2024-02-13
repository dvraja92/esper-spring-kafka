package com.decipherzone.esper.events;

import com.decipherzone.esper.entity.JsonEntity;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Date;

@Component
@Slf4j
@RequiredArgsConstructor
public class JsonEvents {

    private final String[] topics = {"A", "B", "C"};
    private int currentTopicIndex = 0;

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Scheduled(fixedRate = 5000)
    public void produceJsonEvent() {
        String currentTopic = topics[currentTopicIndex];
        JsonEntity jsonEntity = generateJsonEvent(currentTopic);
        kafkaTemplate.send(currentTopic, String.valueOf(jsonEntity.toString()));
        log.info(currentTopic, jsonEntity);
        currentTopicIndex = (currentTopicIndex + 1) % topics.length;
    }

   private JsonEntity generateJsonEvent(String currentTopic) {
       JsonEntity jsonEntity = new JsonEntity();
       jsonEntity.setCurrentDate(new Date());
       jsonEntity.setData("kafkaEsperApplication");
       jsonEntity.setTopic(currentTopic);
       return jsonEntity;
   }

}
