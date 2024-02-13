package com.decipherzone.esper.kafka;

import com.decipherzone.esper.utils.Utils;
import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.common.client.render.JSONEventRenderer;
import com.espertech.esper.runtime.client.EPRuntime;
import com.espertech.esper.runtime.client.EPStatement;
import com.espertech.esper.runtime.client.UpdateListener;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaOutputListener implements UpdateListener {

    private static final Logger log = LoggerFactory.getLogger(KafkaOutputListener.class);

    private final JSONEventRenderer jsonEventRenderer;
    private final KafkaProducer producer;
    private final String[] topics;

    public KafkaOutputListener(EPRuntime runtime, EPStatement statement, KafkaProducer producer, String[] topics) {
        jsonEventRenderer = runtime.getRenderEventService().getJSONRenderer(statement.getEventType());
        this.producer = producer;
        this.topics = topics;
    }
    public void update(EventBean[] newEvents, EventBean[] oldEvents, EPStatement statement, EPRuntime runtime) {

        if (newEvents == null) {
            return;
        }
        for (EventBean event : newEvents) {
            for (String topic : topics) {
                log.info("Sending Event to topic {} {}", topic, event);
                producer.send(new ProducerRecord(topic, Utils.buildOutputString(event)));
            }
        }
    }
}

