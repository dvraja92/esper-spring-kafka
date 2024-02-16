package com.contextsuite.esper.kafka;

import com.contextsuite.esper.provider.ApplicationContextProvider;
import com.espertech.esperio.kafka.EsperIOKafkaInputSubscriber;
import com.espertech.esperio.kafka.EsperIOKafkaInputSubscriberContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class KafkaEventConsumer implements EsperIOKafkaInputSubscriber {

    private static final Logger log = LoggerFactory.getLogger(KafkaEventConsumer.class);

    @Override
    public void subscribe(EsperIOKafkaInputSubscriberContext context) {

        final List<String> inputTopics = ApplicationContextProvider.getEsper(context.getRuntime().getURI()).getInputs();

        log.info("Subscribing to topics {}", inputTopics);
        context.getConsumer().subscribe(inputTopics);
    }
}
