package com.contextsuite.esper.config.websocket;

import org.springframework.http.server.ServerHttpRequest;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.stereotype.Component;

import java.security.Principal;
import java.util.List;

@Component
public class WebSocketSessionValidator {

    public Principal setPrincipal(ServerHttpRequest request) {
        String url = request.getURI().toString();
        String user = request.getHeaders().getFirst("User");
        if (user != null && user.trim().length() > 0) {
            return new UsernamePasswordAuthenticationToken(user, null, List.of());
        }
        return null;
    }

}
