package com.contextsuite.esper.websocket;

import com.contextsuite.esper.service.WebSocketAdapterService;
import com.contextsuite.esper.events.EsperWrappedPersonEvent;
import com.contextsuite.esper.provider.ApplicationContextProvider;
import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.runtime.client.EPRuntime;
import com.espertech.esper.runtime.client.EPStatement;
import com.espertech.esper.runtime.client.UpdateListener;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class WebsocketOutputListener implements UpdateListener {

    private final WebSocketAdapterService socketAdapterService;

    public WebsocketOutputListener() {
        this.socketAdapterService = ApplicationContextProvider.socketAdapterService();
    }

    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents, EPStatement statement, EPRuntime runtime) {

        if (newEvents == null) {
            return;
        }
        for (EventBean event : newEvents) {
            EsperWrappedPersonEvent personEvent = (EsperWrappedPersonEvent) event.getUnderlying();
            for (String topic : ApplicationContextProvider.getEsper(runtime.getURI()).getOutput(personEvent.getSource())) {
                log.info("Sending Event to topic {} {}", topic, event);
                socketAdapterService.sendMessage(personEvent, topic);
            }
        }
    }
}
