package com.contextsuite.esper.provider;

import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.runtime.client.EPRuntime;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Data
@Builder
@RequiredArgsConstructor
@AllArgsConstructor
public class Esper {

    private static final List<String> DEAD_X = List.of("dead");

    private final Configuration configuration;
    private final Map<String, List<String>> mappings;
    private EPRuntime runtime;
    private boolean enableWebSocket;

    public List<String> getOutput(final String input) {
        return mappings.getOrDefault(input, DEAD_X);
    }

    public List<String> getInputs() {
        return new ArrayList<>(mappings.keySet());
    }

}
