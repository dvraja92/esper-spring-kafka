package com.decipherzone.esper.controller;

import com.decipherzone.esper.service.EPRuntimeService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/instance")
@RequiredArgsConstructor
@Slf4j
public class InstanceController {

    private final EPRuntimeService service;

    @GetMapping("/start/{topic}")
    public ResponseEntity<String> start(@PathVariable("topic") String topic) {
        service.runtime(topic).initialize();
        return new ResponseEntity<>("Esper Instance Initialized...", HttpStatus.OK);
    }

    @GetMapping("/stop/{topic}")
    public ResponseEntity<String> stop(@PathVariable("topic") String topic) {
        service.runtime(topic).destroy();
        return new ResponseEntity<>("Esper Instance Stopped...", HttpStatus.OK);
    }
}
