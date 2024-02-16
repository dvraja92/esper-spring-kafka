package com.contextsuite.esper.service;

import com.contextsuite.esper.factory.EPRuntimeFactory;
import com.contextsuite.esper.kafka.KafkaOutputListener;
import com.contextsuite.esper.provider.Esper;
import com.contextsuite.esper.rest.command.runtime.StartRuntimeCommand;
import com.contextsuite.esper.rest.command.runtime.StopRuntimeCommand;
import com.contextsuite.esper.rest.command.statement.AddStatementCommand;
import com.contextsuite.esper.rest.response.ListRuntimeResponse;
import com.contextsuite.esper.utils.EPLUtil;
import com.contextsuite.esper.websocket.WebsocketOutputListener;
import com.contextsuite.esper.rest.response.EsperRuntime;
import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.common.client.configuration.runtime.ConfigurationRuntimePluginLoader;
import com.espertech.esper.runtime.client.EPDeployment;
import com.espertech.esper.runtime.client.EPRuntime;
import com.espertech.esper.runtime.client.EPStatement;
import com.espertech.esper.runtime.client.EPUndeployException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class EPRuntimeService {

    private final EPRuntimeFactory runtimeFactory;

    public void startRuntime(StartRuntimeCommand command) {
        Esper esper = runtimeFactory.get(command.getName());
        if (esper == null) {
            runtimeFactory.getOrInit(command.getName(), command);
        } else if (esper.getRuntime().isDestroyed()) {
            esper.getRuntime().initialize();
        } else {
            // Already started esper
            log.info("Runtime {} is already started", command.getName());
        }
    }

    public boolean stopRuntime(StopRuntimeCommand command) {
        boolean stopped = runtimeFactory.stop(command.getName());
        log.info("Stopping runtime {}, Status : {}", command.getName(), stopped);
        return stopped;
    }

    public ListRuntimeResponse listRuntimes() {
        ListRuntimeResponse.ListRuntimeResponseBuilder builder = ListRuntimeResponse.builder();
        for (Map.Entry<String, Esper> entry : runtimeFactory.getRuntimeMap().entrySet()) {
            EPRuntime runtime = entry.getValue().getRuntime();
            List<EsperRuntime.Statement> statementList = new ArrayList<>();
            for (String deployment : runtime.getDeploymentService().getDeployments()) {
                statementList.addAll(Arrays.stream(runtime.getDeploymentService().getDeployment(deployment).getStatements())
                        .map(es -> EsperRuntime.Statement.builder()
                                .deploymentId(es.getDeploymentId())
                                .name(es.getName())
                                .build()).toList());
            }
            builder.runtime(EsperRuntime.builder()
                    .name(entry.getKey())
                    .mappings(entry.getValue().getMappings())
                    .statements(statementList)
                    .webSocketEnabled(entry.getValue().isEnableWebSocket())
                    .build());
        }

        return builder.build();
    }


    public EsperRuntime.Statement addStatement(String runtimeName, AddStatementCommand command) {
        return addStatement(runtimeName, command, UUID.randomUUID().toString());
    }

    public boolean stopStatement(String runtime, String deploymentId) {

        Esper esper = runtimeFactory.get(runtime);

        if (esper == null) {
            throw new RuntimeException("EPRuntime is not available");
        }

        EPDeployment deployment = esper.getRuntime().getDeploymentService().getDeployment(deploymentId);
        if (deployment == null) {
            throw new RuntimeException("Deployment is not available");
        }

        try {
            esper.getRuntime().getDeploymentService().undeploy(deploymentId);
            return true;
        } catch (EPUndeployException e) {
            log.warn("Unable to undeploy");
            return false;
        }
    }

    public List<EsperRuntime.Statement> listStatements(String runtime) {

        Esper esper = runtimeFactory.get(runtime);

        if (esper == null) {
            throw new RuntimeException("EPRuntime is not available");
        }

        List<EsperRuntime.Statement> statementList = new ArrayList<>();
        for (String deployment : esper.getRuntime().getDeploymentService().getDeployments()) {
            statementList.addAll(Arrays.stream(esper.getRuntime().getDeploymentService().getDeployment(deployment).getStatements())
                    .map(es -> EsperRuntime.Statement.builder()
                            .deploymentId(es.getDeploymentId())
                            .name(es.getName())
                            .build()).toList());
        }

        return statementList;
    }

    public EsperRuntime.Statement updateStatement(String runtime, String deploymentId, AddStatementCommand command) {
        boolean stopped = stopStatement(runtime, deploymentId);

        if (!stopped) {
            throw new RuntimeException("Couldn't update");
        }

        return addStatement(runtime, command, deploymentId);
    }

    private EsperRuntime.Statement addStatement(String runtimeName, AddStatementCommand command, String deploymentId) {

        Esper esper = runtimeFactory.get(runtimeName);

        if (esper == null) {
            throw new RuntimeException("EPRuntime is not available");
        }

        Configuration configuration = esper.getConfiguration();
        log.info("Got statement {}: {}", command.getName(), command.getStatement());
        EPCompiled compiled = EPLUtil.compileEPL(configuration, command.getStatement(), command.getName());
        EPDeployment deployment = EPLUtil.deploy(esper.getRuntime(), compiled, deploymentId);

        EPStatement statement = esper.getRuntime().getDeploymentService().getStatement(deployment.getDeploymentId(), command.getName());
        Optional<ConfigurationRuntimePluginLoader> kafkaOutput = esper.getRuntime().getConfigurationDeepCopy().getRuntime().getPluginLoaders().stream().filter(p -> Objects.equals(p.getLoaderName(), "KafkaOutput")).findFirst();
        if (kafkaOutput.isPresent()) {
            KafkaOutputListener listener = new KafkaOutputListener(esper.getRuntime(), statement, new KafkaProducer<>(kafkaOutput.get().getConfigProperties()));
            statement.addListener(listener);
            log.info("Kafka Output Listener added");
        } else {
            log.info("Kafka Output can't be added");
        }
        if (esper.isEnableWebSocket()) {
            statement.addListener(new WebsocketOutputListener());
            log.info("WebSocket Output Listener added");
        } else {
            log.warn("WebSocket Output is disabled as per configuration");
        }

        return EsperRuntime.Statement.builder()
                .deploymentId(deployment.getDeploymentId())
                .name(statement.getName())
                .build();

    }
}
