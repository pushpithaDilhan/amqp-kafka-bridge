package io.strimzi.kafka.bridge.http;

import io.strimzi.kafka.bridge.Endpoint;
import io.strimzi.kafka.bridge.SinkBridgeEndpoint;
import io.strimzi.kafka.bridge.config.BridgeConfigProperties;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerRequest;

public class HttpSinkBridgeEndpoint extends SinkBridgeEndpoint<K, V>{

        /**
         * Constructor
         *
         * @param vertx                  Vert.x instance
         * @param bridgeConfigProperties Bridge configuration
         */
        public HttpSinkBridgeEndpoint(Vertx vertx, BridgeConfigProperties bridgeConfigProperties) {
                super(vertx, bridgeConfigProperties);
        }

        @Override
        public void open() {

        }

        @Override
        public void handle(Endpoint<?> endpoint) {

        }
}
