package com.decipherzone.esper.utils;

import com.espertech.esper.common.client.EventBean;

public class Utils {

    public static String buildOutputString(EventBean event) {
        return event.get("word").toString() + " (" + event.get("len") + ")";
    }
}
