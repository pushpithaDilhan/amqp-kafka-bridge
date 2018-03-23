package io.strimzi.kafka.bridge.example.http;

import io.strimzi.kafka.bridge.http.HttpBridge;
import io.vertx.core.Vertx;

import java.io.IOException;

public class HttpBridgeServer {

    public static void main(String[] args) {

        Vertx vertx = Vertx.vertx();
        HttpBridge bridge = new HttpBridge();
        vertx.deployVerticle(bridge);
        try {
            System.in.read();
            vertx.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
