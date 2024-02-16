package com.contextsuite.esper.provider;

import com.contextsuite.esper.factory.EPRuntimeFactory;
import com.contextsuite.esper.service.WebSocketAdapterService;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

@Component
public class ApplicationContextProvider implements ApplicationContextAware {

    private static ApplicationContext applicationContext;

    public static ApplicationContext context() {
        return applicationContext;
    }

    public static Esper getEsper(String name) {
        return applicationContext.getBean(EPRuntimeFactory.class).get(name);
    }

    public static WebSocketAdapterService socketAdapterService() {
        return applicationContext.getBean(WebSocketAdapterService.class);
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        ApplicationContextProvider.applicationContext = applicationContext;
    }

}
