package com.decipherzone.esper.kafka;

import com.espertech.esperio.kafka.EsperIOKafkaInputSubscriber;
import com.espertech.esperio.kafka.EsperIOKafkaInputSubscriberContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;;

import java.util.Arrays;
import java.util.Map;

import static com.decipherzone.esper.config.EPRuntimeWrapperConfiguration.INPUT_TOPIC;

public class KafkaEventConsumer implements EsperIOKafkaInputSubscriber {

    private static final Logger log = LoggerFactory.getLogger(KafkaEventConsumer.class);

    private String[] inputTopic;

    private void setInputTopic(EsperIOKafkaInputSubscriberContext context) {

        if (inputTopic == null) {
            inputTopic = new String[]{context.getRuntime().getVariableService()
                    .getVariableValueAll()
                    .entrySet()
                    .stream()
                    .filter(d -> INPUT_TOPIC.equals(d.getKey().getName())).findFirst().map(Map.Entry::getValue).orElse("dead").toString()};
        }
    }


    @Override
    public void subscribe(EsperIOKafkaInputSubscriberContext context) {
        setInputTopic(context);
        log.info("Subscribing to topics {}", Arrays.toString(inputTopic));
        context.getConsumer().subscribe(Arrays.asList(inputTopic));
    }
}
