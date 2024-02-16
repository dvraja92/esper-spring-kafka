package com.contextsuite.esper.rest;

import com.contextsuite.esper.rest.command.runtime.StartRuntimeCommand;
import com.contextsuite.esper.rest.command.runtime.StopRuntimeCommand;
import com.contextsuite.esper.rest.response.ListRuntimeResponse;
import com.contextsuite.esper.service.EPRuntimeService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/instance")
@RequiredArgsConstructor
@Slf4j
public class InstanceController {

    private final EPRuntimeService service;

    @PostMapping("/start")
    public ResponseEntity<String> start(@Valid @RequestBody final StartRuntimeCommand command) {
        service.startRuntime(command);
        return new ResponseEntity<>("Esper Instance Initialized...", HttpStatus.OK);
    }

    @PostMapping("/stop")
    public ResponseEntity<String> stop(@Valid @RequestBody final StopRuntimeCommand command) {
        return new ResponseEntity<>("Esper Instance Stopped ? -> " + service.stopRuntime(command), HttpStatus.OK);
    }

    @GetMapping
    public ResponseEntity<ListRuntimeResponse> list() {
        return ResponseEntity.ok(service.listRuntimes());
    }
}
