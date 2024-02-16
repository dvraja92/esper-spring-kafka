package com.contextsuite.esper.rest.response;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class EsperRuntime {

    private String name;

    private boolean webSocketEnabled;

    private Map<String, List<String>> mappings;

    private List<Statement> statements;

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Statement {
        private String deploymentId;

        private String name;
    }

}
