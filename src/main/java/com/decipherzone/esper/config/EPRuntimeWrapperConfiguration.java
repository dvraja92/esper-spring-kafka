package com.decipherzone.esper.config;


import com.decipherzone.esper.kafka.KafkaEventConsumer;
import com.decipherzone.esper.kafka.KafkaEventProducer;
import com.decipherzone.esper.utils.EPLUtil;
import com.decipherzone.esper.kafka.KafkaInputProcessor;
import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.common.client.module.Module;
import com.espertech.esper.common.client.module.ParseException;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.DeploymentOptions;
import com.espertech.esper.runtime.client.EPDeployException;
import com.espertech.esper.runtime.client.EPDeployment;
import com.espertech.esper.runtime.client.EPRuntime;
import com.espertech.esper.runtime.client.EPRuntimeProvider;
import com.espertech.esperio.kafka.EsperIOKafkaConfig;
import com.espertech.esperio.kafka.EsperIOKafkaInputAdapterPlugin;
import com.espertech.esperio.kafka.EsperIOKafkaInputTimestampExtractorConsumerRecord;
import com.espertech.esperio.kafka.EsperIOKafkaOutputAdapterPlugin;
import com.espertech.esperio.kafka.KafkaOutputDefault;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static com.decipherzone.esper.config.KafkaConfig.BOOTSTRAP_ADDRESS;
import static com.decipherzone.esper.config.KafkaConfig.INPUT_TOPICS;
import static com.decipherzone.esper.config.KafkaConfig.OUTPUT_TOPICS;

@Slf4j
@org.springframework.context.annotation.Configuration
public class EPRuntimeWrapperConfiguration {

    public final static String OUTPUT_TOPIC = "OUTPUT_TOPIC";
    public final static String INPUT_TOPIC = "INPUT_TOPIC";

    @Bean
    public Map<String, EPRuntimeWrapper> wrappersConfig() {
        if (INPUT_TOPICS.length != OUTPUT_TOPICS.length) {
            throw new IllegalArgumentException("In/Out topics do not match");
        }

        Map<String, EPRuntimeWrapper> map = new HashMap<>();

        for (int i = 0; i < INPUT_TOPICS.length; i++) {

            final String inputTopic = INPUT_TOPICS[i];
            final String outputTopic = OUTPUT_TOPICS[i];
            Configuration configuration = esperConfiguration(inputTopic, outputTopic);
            EPCompiled epCompiled = epCompiled(configuration);
            EPRuntime epRuntime = epRuntime(configuration, inputTopic);
            EPDeployment epDeployment = epDeployment(epRuntime, epCompiled);
            EPRuntimeWrapper epRuntimeWrapper = EPRuntimeWrapper.builder()
                    .configuration(configuration)
                    .epCompiled(epCompiled)
                    .epDeployment(epDeployment)
                    .runtime(epRuntime)
                    .inputTopic(inputTopic)
                    .outputTopic(outputTopic)
                    .build();

            map.putIfAbsent(inputTopic, epRuntimeWrapper);
        }

        return map;

    }

    public final static String RUNTIME_URI = "EsperKafka-%s";
    private static EPCompiled compileModule(Module module, Configuration configuration) {
        try {
            return EPCompilerProvider.getCompiler().compile(module, new CompilerArguments(configuration));
        } catch (EPCompileException ex) {
            throw new RuntimeException(ex);
        }
    }

    private static Properties getConsumerProps() {
        Properties props = new Properties();

        // Kafka Consumer Properties
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_ADDRESS);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.toString().toLowerCase());

        // EsperIO Kafka Input Adapter Properties
        props.put(EsperIOKafkaConfig.INPUT_SUBSCRIBER_CONFIG, KafkaEventConsumer.class.getName());
        props.put(EsperIOKafkaConfig.INPUT_PROCESSOR_CONFIG, KafkaInputProcessor.class.getName());
        props.put(EsperIOKafkaConfig.INPUT_TIMESTAMPEXTRACTOR_CONFIG, EsperIOKafkaInputTimestampExtractorConsumerRecord.class.getName());

        return props;
    }

    private static Properties getProducerProps() {
        Properties props = new Properties();

        // Kafka Producer Properties
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_ADDRESS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // EsperIO Kafka Output Adapter Properties
        props.put(EsperIOKafkaConfig.OUTPUT_FLOWCONTROLLER_CONFIG, KafkaEventProducer.class.getName());

        return props;
    }

    private Configuration esperConfiguration(final String inputTopic, final String outputTopic) {
        Configuration configuration = new Configuration();
        configuration.configure("esper.cfg.xml");
        configuration.getCommon().addImport(KafkaOutputDefault.class);
        configuration.getCommon().addVariable(OUTPUT_TOPIC, String.class, outputTopic);
        configuration.getCommon().addVariable(INPUT_TOPIC, String.class, inputTopic);
        configuration.getCommon().addEventType(String.class);
        configuration.getRuntime().addPluginLoader("KafkaInput", EsperIOKafkaInputAdapterPlugin.class.getName(), getConsumerProps(), null);
        configuration.getRuntime().addPluginLoader("KafkaOutput", EsperIOKafkaOutputAdapterPlugin.class.getName(), getProducerProps(), null);
        configuration.getRuntime().getVariables();
        log.info("Configuration is loaded ({})", configuration.toString());
        return configuration;
    }

    private EPCompiled epCompiled(Configuration configuration) {
        URL url = EPLUtil.class.getResource("/queries/queries.epl");
        Module queries = null;
        try {
            queries = EPCompilerProvider.getCompiler().readModule(url);
        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }

        return compileModule(queries, configuration);
    }

    private EPRuntime epRuntime(Configuration configuration, String inputTopic) {
        return EPRuntimeProvider.getRuntime(String.format(RUNTIME_URI, inputTopic), configuration);
    }

    private EPDeployment epDeployment(EPRuntime runtime, EPCompiled compiled) {
        EPDeployment deployment;
        try {
            deployment = runtime.getDeploymentService().deploy(compiled, new DeploymentOptions().setDeploymentId(UUID.randomUUID().toString()));
        } catch (EPDeployException ex) {
            throw new RuntimeException(ex);
        }
        return deployment;
    }


}
