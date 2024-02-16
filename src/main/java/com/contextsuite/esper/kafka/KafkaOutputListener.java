package com.contextsuite.esper.kafka;

import com.contextsuite.esper.events.EsperWrappedPersonEvent;
import com.contextsuite.esper.provider.ApplicationContextProvider;
import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.runtime.client.EPRuntime;
import com.espertech.esper.runtime.client.EPStatement;
import com.espertech.esper.runtime.client.UpdateListener;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaOutputListener implements UpdateListener {

    private static final Logger log = LoggerFactory.getLogger(KafkaOutputListener.class);

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final KafkaProducer producer;

    public KafkaOutputListener(EPRuntime runtime, EPStatement statement, KafkaProducer producer) {
        this.producer = producer;
    }
    public void update(EventBean[] newEvents, EventBean[] oldEvents, EPStatement statement, EPRuntime runtime) {

        if (newEvents == null) {
            return;
        }
        for (EventBean event : newEvents) {
            EsperWrappedPersonEvent personEvent = (EsperWrappedPersonEvent) event.getUnderlying();
            for (String topic : ApplicationContextProvider.getEsper(runtime.getURI()).getOutput(personEvent.getSource())) {
                log.info("Sending Event to topic {} {}", topic, event);
                try {
                    producer.send(new ProducerRecord<>(topic, objectMapper.writeValueAsString(personEvent)));
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}

