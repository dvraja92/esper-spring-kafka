package com.contextsuite.esper.factory;

import com.contextsuite.esper.config.KafkaConfig;
import com.contextsuite.esper.kafka.KafkaEventConsumer;
import com.contextsuite.esper.kafka.KafkaEventProducer;
import com.contextsuite.esper.kafka.KafkaInputProcessor;
import com.contextsuite.esper.provider.Esper;
import com.contextsuite.esper.rest.command.runtime.StartRuntimeCommand;
import com.contextsuite.esper.utils.EPLUtil;
import com.contextsuite.esper.events.EsperWrappedPersonEvent;
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
import jakarta.annotation.Nullable;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class EPRuntimeFactory {

    @Getter
    private final Map<String, Esper> runtimeMap = new HashMap<>();

    public @Nullable Esper get(String name) {
        return runtimeMap.get(name);
    }

    public boolean stop(String name) {
        Esper esper = runtimeMap.get(name);
        if (esper == null) {
            return true;
        }

        try {
            esper.getRuntime().destroy();
            runtimeMap.remove(name);
            return true;
        } catch (Exception e) {
            log.warn("Unable to stop runtime {}", name, e);
        }
        return false;
    }

    public Esper getOrInit(String name, StartRuntimeCommand command) {
        if (runtimeMap.containsKey(name)) {
            return get(name);
        }

        // Init a new Runtime

        final Configuration configuration = configuration();
        final EPCompiled compiled = compiled(configuration);

        Esper esper = Esper.builder()
                .enableWebSocket(command.isEnableWebsocket())
                .mappings(command.getMappings())
                .configuration(configuration)
                .build();
        runtimeMap.put(command.getName(), esper);

        final EPRuntime runtime = runtime(configuration, command.getName());
        esper.setRuntime(runtime);
        final EPDeployment deployment = deployment(runtime, compiled);

        return get(name);
    }

    private Configuration configuration() {
        Configuration configuration = new Configuration();
        configuration.configure("esper.cfg.xml");
        configuration.getCommon().addImport(KafkaOutputDefault.class);
        configuration.getCommon().addEventType(String.class);
        configuration.getCommon().addEventType("PersonEvent", EsperWrappedPersonEvent.class);
        configuration.getRuntime().addPluginLoader("KafkaInput", EsperIOKafkaInputAdapterPlugin.class.getName(), getConsumerProps(), null);
        configuration.getRuntime().addPluginLoader("KafkaOutput", EsperIOKafkaOutputAdapterPlugin.class.getName(), getProducerProps(), null);
        log.info("Configuration is loaded ({})", configuration);
        return configuration;
    }

    private EPCompiled compiled(Configuration configuration) {
        URL url = EPLUtil.class.getResource("/queries/queries.epl");
        Module queries = null;
        try {
            queries = EPCompilerProvider.getCompiler().readModule(url);
        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }

        return compileModule(queries, configuration);
    }

    private EPDeployment deployment(EPRuntime runtime, EPCompiled compiled) {
        EPDeployment deployment;
        try {
            deployment = runtime.getDeploymentService().deploy(compiled, new DeploymentOptions().setDeploymentId(UUID.randomUUID().toString()));
        } catch (EPDeployException ex) {
            throw new RuntimeException(ex);
        }
        return deployment;
    }

    private EPRuntime runtime(Configuration configuration, String name) {
        return EPRuntimeProvider.getRuntime(name, configuration);
    }

    private EPCompiled compileModule(Module module, Configuration configuration) {
        try {
            return EPCompilerProvider.getCompiler().compile(module, new CompilerArguments(configuration));
        } catch (EPCompileException ex) {
            throw new RuntimeException(ex);
        }
    }

    private Properties getConsumerProps() {
        Properties props = new Properties();

        // Kafka Consumer Properties
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAP_ADDRESS);
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

    private Properties getProducerProps() {
        Properties props = new Properties();

        // Kafka Producer Properties
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAP_ADDRESS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // EsperIO Kafka Output Adapter Properties
        props.put(EsperIOKafkaConfig.OUTPUT_FLOWCONTROLLER_CONFIG, KafkaEventProducer.class.getName());

        return props;
    }

}
