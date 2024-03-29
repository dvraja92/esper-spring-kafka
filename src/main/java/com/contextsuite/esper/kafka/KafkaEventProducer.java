package com.contextsuite.esper.kafka;

import com.contextsuite.esper.provider.ApplicationContextProvider;
import com.contextsuite.esper.provider.Esper;
import com.contextsuite.esper.websocket.WebsocketOutputListener;
import com.espertech.esper.common.internal.epl.annotation.AnnotationUtil;
import com.espertech.esper.runtime.client.DeploymentStateEventDeployed;
import com.espertech.esper.runtime.client.DeploymentStateEventUndeployed;
import com.espertech.esper.runtime.client.DeploymentStateListener;
import com.espertech.esper.runtime.client.EPRuntime;
import com.espertech.esper.runtime.client.EPStatement;
import com.espertech.esper.runtime.client.UpdateListener;
import com.espertech.esperio.kafka.EsperIOKafkaOutputFlowController;
import com.espertech.esperio.kafka.EsperIOKafkaOutputFlowControllerContext;
import com.espertech.esperio.kafka.KafkaOutputDefault;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.util.Iterator;

public class KafkaEventProducer implements EsperIOKafkaOutputFlowController {

    private static final Logger log = LoggerFactory.getLogger(KafkaEventProducer.class);

    private KafkaProducer producer;
    private EPRuntime runtime;

    private Esper esper;

    @Override
    public void initialize(EsperIOKafkaOutputFlowControllerContext context) {
        this.runtime = context.getRuntime();
        esper = ApplicationContextProvider.getEsper(this.runtime.getURI());
        // obtain producer
        try {
            producer = new KafkaProducer<>(context.getProperties());
        } catch (Throwable t) {
            log.error("Error obtaining Kafka producer for URI '{}': {}", context.getRuntime().getURI(), t.getMessage(), t);
        }

        // attach to existing statements
        String[] deploymentIds = context.getRuntime().getDeploymentService().getDeployments();
        for (String deploymentId : deploymentIds) {
            EPStatement[] statements = context.getRuntime().getDeploymentService().getDeployment(deploymentId).getStatements();
            for (EPStatement statement : statements) {
                processStatement(statement);
            }
        }

        // attach listener to receive newly-created statements
        runtime.getDeploymentService().addDeploymentStateListener(new DeploymentStateListener() {
            public void onDeployment(DeploymentStateEventDeployed event) {
                for (EPStatement statement : event.getStatements()) {
                    processStatement(statement);
                }
            }

            public void onUndeployment(DeploymentStateEventUndeployed event) {
                for (EPStatement statement : event.getStatements()) {
                    detachStatement(statement);
                }
            }
        });
    }

    private void processStatement(EPStatement statement) {
        if (statement == null) {
            return;
        }
        Annotation annotation = AnnotationUtil.findAnnotation(statement.getAnnotations(), KafkaOutputDefault.class);
        if (annotation == null) {
            return;
        }

        KafkaOutputListener listener = new KafkaOutputListener(runtime, statement, producer);
        statement.addListener(listener);
        if (this.esper.isEnableWebSocket()) {
            statement.addListener(new WebsocketOutputListener());
        }
    }

    private void detachStatement(EPStatement statement) {
        Iterator<UpdateListener> listeners = statement.getUpdateListeners();
        UpdateListener found = null;
        while (listeners.hasNext()) {
            UpdateListener listener = listeners.next();
            if (listener instanceof KafkaOutputListener) {
                found = listener;
                break;
            }
        }
        if (found != null) {
            statement.removeListener(found);
        }
        log.info("Removed Kafka-Output-Adapter listener from statement '{}'", statement.getName());
    }


    @Override
    public void close() {
        producer.close();
    }

    public void produceMessage(String message, String topic) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                log.info("Message sent successfully to topic: {}", metadata.topic());
            } else {
                log.error("Error sending message: {}", exception.getMessage());
            }
        });
    }
}
