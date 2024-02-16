package com.contextsuite.esper.rest;

import com.contextsuite.esper.rest.command.statement.AddStatementCommand;
import com.contextsuite.esper.service.EPRuntimeService;
import com.contextsuite.esper.rest.response.EsperRuntime;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/statement")
@RequiredArgsConstructor
@Slf4j
public class StatementController {

    private final EPRuntimeService service;

    @PostMapping("/{runtime}")
    public ResponseEntity<EsperRuntime.Statement> addStatement(@PathVariable String runtime, @RequestBody AddStatementCommand command) {
        try {
            return ResponseEntity.ok(service.addStatement(runtime, command));
        } catch (Exception e) {
            log.error("Could not compile statement", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
        }
    }

    @PutMapping("/{runtime}/{deploymentId}")
    public ResponseEntity<EsperRuntime.Statement> updateStatement(@PathVariable String runtime,
                                                                  @PathVariable String deploymentId,
                                                                  @RequestBody AddStatementCommand command) {
        try {
            return ResponseEntity.ok(service.updateStatement(runtime, deploymentId, command));
        } catch (Exception e) {
            log.error("Could not compile statement", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
        }
    }

    @DeleteMapping("/{runtime}/{deploymentId}")
    public ResponseEntity<String> stopStatement(@PathVariable String runtime, @PathVariable String deploymentId) {
        return ResponseEntity.ok(String.format("Triggered removal, status : %s", service.stopStatement(runtime, deploymentId)));
    }

    @GetMapping("/{runtime}/statements")
    public ResponseEntity<List<EsperRuntime.Statement>> listStatement(@PathVariable("runtime") String runtime) {
        return ResponseEntity.ok(service.listStatements(runtime));
    }
}
