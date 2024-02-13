package com.decipherzone.esper.service;

import com.decipherzone.esper.config.EPRuntimeWrapper;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.runtime.client.EPRuntime;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@RequiredArgsConstructor
public class EPRuntimeService {

    private final Map<String, EPRuntimeWrapper> runtimeWrapperMap;


    public EPRuntime runtime(String id) {
        return runtimeWrapperMap.get(id).getRuntime();
    }

    public Configuration configuration(String id) {
        return runtimeWrapperMap.get(id).getConfiguration();
    }
}
