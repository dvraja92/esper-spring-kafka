package com.decipherzone.esper.config;

import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.runtime.client.EPDeployment;
import com.espertech.esper.runtime.client.EPRuntime;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@Builder
@RequiredArgsConstructor
public class EPRuntimeWrapper {

    private final Configuration configuration;

    private final EPCompiled epCompiled;

    private final EPDeployment epDeployment;

    private final EPRuntime runtime;

    private final String inputTopic;

    private final String outputTopic;

}
