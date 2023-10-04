package org.apache.ignite.util;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

public class InternalDebug {

    private static final Map<String, InternalDebug> map = new HashMap<>();

    public static InternalDebug get(String name) {
        if (!map.containsKey(name)) map.put(name, new InternalDebug(name));
        return map.get(name);
    }

    public static InternalDebug once(String name) {
        return new InternalDebug(name);
    }

    private long startTime;
    private final String name;

    private long counter;

    InternalDebug(String name) {
        this.name = name;
    }

    public void start() {
        startTime = System.currentTimeMillis();
    }

    public void startOnce() {
        if (startTime == 0L) start();
    }

    public void log(String msg, PrintStream str) {
        if (!"true".equalsIgnoreCase(System.getenv("MD_QUERY_LOGGING"))) return;
        if (startTime == 0L) return;
        long time = System.currentTimeMillis();
        str.printf("[TIMER LOG || %s] %s: %sms\n", name, msg, time - startTime);
    }

    public void reset() {
        startTime = 0L;
        counter = 0;
    }

    public void counterInc() {
        setCounter(getCounter() + 1);
    }

    public void counterAdd(long l) {
        setCounter(getCounter() + l);
    }

    public void counterSub(long l) {
        setCounter(getCounter() - l);
    }

    public void logCounter(String msg, PrintStream str) {
        if ("true".equals(System.getenv("MD_QUERY_LOGGING"))) {
            str.printf("[TIMER LOG || %s] %s: %s\n", name, msg, getCounter());
        }
    }

    public long getCounter() {
        return counter;
    }

    public void setCounter(long counter) {
        this.counter = counter;
    }

    public static void log(String message) {
        if ("true".equals(System.getenv("MD_QUERY_LOGGING"))) {
            System.out.println(message);
        }
    }
}
