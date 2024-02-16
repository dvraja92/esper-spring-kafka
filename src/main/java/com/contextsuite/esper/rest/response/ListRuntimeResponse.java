package com.contextsuite.esper.rest.response;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Singular;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ListRuntimeResponse {

    @Singular
    private List<EsperRuntime> runtimes;

}
