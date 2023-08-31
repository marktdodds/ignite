package org.apache.ignite.util;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

public class InternalTimer {

    private static final Map<String, InternalTimer> map = new HashMap<>();

    public static InternalTimer get(String name) {
        if (!map.containsKey(name)) map.put(name, new InternalTimer(name));
        return map.get(name);
    }

    private long startTime;
    private String name;

    InternalTimer(String name) {
        this.name = name;
    }

    public void start() {
        startTime = System.currentTimeMillis();
    }

    public void log(String msg, PrintStream str) {
        if (System.getenv("MD_QUERY_LOGGING") == null) return;
        long time = System.currentTimeMillis();
        str.printf("[TIMER LOG || %s] %s: %sms\n", name, msg, time - startTime);
    }

}
