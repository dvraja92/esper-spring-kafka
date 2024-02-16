package com.contextsuite.esper.utils;

import com.contextsuite.esper.events.PersonEvent;
import com.espertech.esper.common.client.EventBean;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Utils {

    public static String buildOutputString(EventBean event) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        PersonEvent personEvent = PersonEvent.builder()
                .age((Integer) event.get("age"))
                .name(event.get("name").toString())
                .build();
        return objectMapper.writeValueAsString(personEvent);
    }

}
