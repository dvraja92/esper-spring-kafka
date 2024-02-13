package com.decipherzone.esper.controller;

import com.decipherzone.esper.controller.response.StatementResponse;
import com.decipherzone.esper.entity.Statement;
import com.decipherzone.esper.service.EPRuntimeService;
import com.decipherzone.esper.utils.EPLUtil;
import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.runtime.client.EPDeployment;
import com.espertech.esper.runtime.client.EPRuntime;
import com.espertech.esper.runtime.client.EPStatement;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/statement")
@RequiredArgsConstructor
@Slf4j
public class StatementController {

    private final EPRuntimeService service;

    @PostMapping("/add-statement/{topic}")
    public ResponseEntity<String> addStatement(@PathVariable("topic")String topic, @RequestBody Statement statements) {
        try {

            final EPRuntime runtime = service.runtime(topic);
            Configuration configuration = service.configuration(topic);

            log.info("Got statement {}: {}", statements.getName(), statements.getStatement());
            EPCompiled compiled = EPLUtil.compileEPL(configuration, statements.getStatement(), statements.getName());

            EPDeployment deployment = EPLUtil.deploy(runtime, compiled);

            EPStatement s = runtime.getDeploymentService().getStatement(deployment.getDeploymentId(), statements.getName());
            s.addListener((newData, oldData, sta, run) -> {
                for (EventBean nd : newData) {
                    log.info("Read {} from Kafka using posted statement ({})", nd.toString(), statements.getName());
                }
            });
            return new ResponseEntity(deployment.getDeploymentId(), HttpStatus.OK);
        } catch (Exception e) {
            log.error("Could not compile statement", e);
            return new ResponseEntity(HttpStatus.NOT_ACCEPTABLE);
        }
    }
    @GetMapping("/list-statement/{topic}")
    public ResponseEntity<List<StatementResponse>> listStatement(@PathVariable("topic")String topic) {
        final EPRuntime runtime = service.runtime(topic);

        List<StatementResponse> statements = Arrays.stream(runtime.getDeploymentService().getDeployments())
                .flatMap(deploymentId -> Arrays.stream(runtime.getDeploymentService().getDeployment(deploymentId).getStatements()))
                .map(stmt -> StatementResponse.builder()
                        .name(stmt.getName())
                        .deploymentId(stmt.getDeploymentId())
                        .isDestroyed(stmt.isDestroyed())
                        .build())
                .collect(Collectors.toList());

        return new ResponseEntity<>(statements, HttpStatus.OK);
    }
}
