package org.apache.ignite.util;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;

public class PropertyServer implements Runnable {

    public static void main(String[] args) {
        new PropertyServer().run();
    }


    public void run() {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(19871), 0);
            server.createContext("/", new SetHandler());
            server.setExecutor(null); // creates a default executor
            server.start();
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
    }

    static class SetHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange t) throws IOException {
            String key = t.getRequestHeaders().getFirst("key");
            String val = t.getRequestHeaders().getFirst("value");
            System.setProperty(key, val);
            String response = "Set " + key + " to: " + val;
            InternalDebug.alwaysLog(response);
            t.sendResponseHeaders(200, response.length());
            OutputStream os = t.getResponseBody();
            os.write(response.getBytes());
            os.close();
        }
    }

}
