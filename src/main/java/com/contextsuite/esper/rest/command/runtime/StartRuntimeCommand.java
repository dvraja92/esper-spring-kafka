package com.contextsuite.esper.rest.command.runtime;

import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
@Builder
@RequiredArgsConstructor
@AllArgsConstructor
public class StartRuntimeCommand {

    @NotBlank
    private final String name;

    private Map<String, List<String>> mappings = new HashMap<>();

    private boolean enableWebsocket;

}
