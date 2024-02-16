package com.contextsuite.esper.kafka;

import com.contextsuite.esper.events.EsperWrappedPersonEvent;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.runtime.client.EPRuntime;
import com.espertech.esperio.kafka.EsperIOKafkaInputProcessor;
import com.espertech.esperio.kafka.EsperIOKafkaInputProcessorContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaInputProcessor implements EsperIOKafkaInputProcessor {

    private static final Logger log = LoggerFactory.getLogger(KafkaInputProcessor.class);

    private EPRuntime runtime;

    private Configuration configurationCopy;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void init(EsperIOKafkaInputProcessorContext context) {
        log.info("Initializing input processor");
        this.runtime = context.getRuntime();
        this.configurationCopy = this.runtime.getConfigurationDeepCopy();
    }

    @Override
    public void process(ConsumerRecords<Object, Object> records) {
        for (ConsumerRecord record : records) {

            if (record.value() != null) {
                log.info("Processing record with value {} ({})", record.value().toString(), record.toString());

                if (log.isDebugEnabled()) {
                    log.debug("Sending event {}", record.value().toString());
                }

                try {
                    log.info("Event Type Names: {}", configurationCopy.getCommon().getEventTypeNames());
                    EsperWrappedPersonEvent personEvent = objectMapper.readValue(record.value().toString(), EsperWrappedPersonEvent.class);
                    personEvent.setSource(record.topic());
                    runtime.getEventService().sendEventBean(personEvent, "PersonEvent");
                } catch (Exception e) {
                    log.error("Could not send Event", e);
                }
            }
        }
    }

    public void close() {}
}

