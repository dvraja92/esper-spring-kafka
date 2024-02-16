package com.contextsuite.esper.service;

import com.contextsuite.esper.events.PersonEvent;
import com.contextsuite.esper.config.websocket.WebSocketConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.TextMessage;

import java.io.IOException;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class WebSocketAdapterService {

    private final Map<String, WebSocketConfig.SessionWrapper> sessions;
    private final ObjectMapper objectMapper;

    public void sendMessage(PersonEvent personEvent, String output) {
        for (Map.Entry<String, WebSocketConfig.SessionWrapper> entry : sessions.entrySet()) {
            WebSocketConfig.SessionWrapper sessionWrapper = entry.getValue();
            if (sessionWrapper.getOutputTopics().contains(output)) {
                try {
                    sessionWrapper.getSession().sendMessage(new TextMessage(objectMapper.writeValueAsString(personEvent)));
                } catch (IOException e) {
                    log.error("Unable to send to WS", e);
                }
            }
        }
    }

}
