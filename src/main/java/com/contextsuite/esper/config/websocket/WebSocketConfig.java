package com.contextsuite.esper.config.websocket;

import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.MultiValueMap;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;
import org.springframework.web.socket.handler.AbstractWebSocketHandler;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Optional.ofNullable;
import static org.springframework.web.socket.CloseStatus.SERVER_ERROR;

@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {

    private final Map<String, SessionWrapper> sessions = new HashMap<>();

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(new EsperWebSocketHandler(sessions), "/ws/outputs")
                .setAllowedOrigins("*")
                .setHandshakeHandler(new WebsocketAuthenticationHandshakeHandler(getWebSocketSessionValidator(), "setPrincipal"));
    }

    @Bean
    public Map<String, SessionWrapper> getSessions() {
        return sessions;
    }

    @Bean
    public WebSocketSessionValidator getWebSocketSessionValidator() {
        return new WebSocketSessionValidator();
    }


    @RequiredArgsConstructor
    public static class EsperWebSocketHandler extends AbstractWebSocketHandler {

        private final Map<String, SessionWrapper> sessions;

        @Override
        public void afterConnectionEstablished(WebSocketSession session) throws Exception {
            var principal = session.getPrincipal();
            MultiValueMap<String, String> queryParams = UriComponentsBuilder.fromUri(Objects.requireNonNull(session.getUri())).build().getQueryParams();

            final List<String> outputTopics = ofNullable(queryParams.get("topic")).orElse(List.of())
                    .stream().filter(Objects::nonNull).filter(s -> s.trim().length() > 0).toList();

            if (principal == null) {
                session.close(SERVER_ERROR.withReason("Header: 'User' is missing"));
                return;
            }

            if (outputTopics.isEmpty()) {
                session.close(SERVER_ERROR.withReason("'topic' can not be blank"));
                return;
            }

            final SessionWrapper sessionWrapper = SessionWrapper.builder()
                    .session(session)
                    .user(principal.getName())
                    .outputTopics(outputTopics)
                    .build();

            sessions.put(principal.getName(), sessionWrapper);
        }

        @Override
        public void afterConnectionClosed(WebSocketSession session, CloseStatus status) throws Exception {
            var principal = session.getPrincipal();
            sessions.remove(principal.getName());
        }
    }


    @Getter
    @RequiredArgsConstructor
    @Builder
    public static class SessionWrapper {
        private final String user;
        private final List<String> outputTopics;
        private final WebSocketSession session;
    }

}
