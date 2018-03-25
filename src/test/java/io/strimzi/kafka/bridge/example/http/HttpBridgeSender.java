package io.strimzi.kafka.bridge.example.http;

import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.Buffer;

public class HttpBridgeSender {
        private static final Logger log = LoggerFactory.getLogger(HttpBridgeSender.class);
        private static final String BRIDGE_HOST = "localhost";
        private static final int BRIDGE_PORT = 9000;
        private static final String BRIDGE_URI = "/";

        public static void main(String[] args) {

                Vertx vertx = Vertx.vertx();
                WebClient client = WebClient.create(vertx);

                client
                        .post(BRIDGE_PORT, BRIDGE_HOST, BRIDGE_URI)
                        // send message to a topic as a JSON object
                        .sendJsonObject(new JsonObject()
                                .put("topic", "my_topic")
                                .put("message", "Simple message from Pushpitha"),
                                ar -> {
                                if (ar.succeeded()) {
                                        log.info("Status code : {}", ar.result().statusCode());
                                }
                        });

        }

}
