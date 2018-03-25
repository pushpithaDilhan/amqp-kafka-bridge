package io.strimzi.kafka.bridge.example.http;

import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpBridgeReceiver {
        private static final Logger log = LoggerFactory.getLogger(HttpBridgeReceiver.class);
        private static final String BRIDGE_HOST = "localhost";
        private static final int BRIDGE_PORT = 9000;
        private static final String BRIDGE_URI = "/";

        public static void main(String[] args) {
                Vertx vertx = Vertx.vertx();
                WebClient client = WebClient.create(vertx);

                client
                        .get(BRIDGE_PORT, BRIDGE_HOST, BRIDGE_URI)
                        .addQueryParam("topics", "my_topic")
                        .send(ar -> {
                                if (ar.succeeded()) {
                                        // Obtain response
                                        HttpResponse<Buffer> response = ar.result();
                                        System.out.println("Received response " + response.toString());
                                } else {
                                        System.out.println("Something went wrong " + ar.cause().getMessage());
                                }
                        });
        }
}
