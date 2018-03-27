package io.strimzi.kafka.bridge.http;

import io.strimzi.kafka.bridge.Endpoint;
import io.strimzi.kafka.bridge.SourceBridgeEndpoint;
import io.strimzi.kafka.bridge.config.BridgeConfigProperties;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.producer.KafkaProducerRecord;

public class HttpSourceBridgeEndpoint extends SourceBridgeEndpoint{

        /**
         * Constructor
         *
         * @param vertx                  Vert.x instance
         * @param bridgeConfigProperties Bridge configuration
         */
        public HttpSourceBridgeEndpoint(Vertx vertx, BridgeConfigProperties bridgeConfigProperties) {
                super(vertx, bridgeConfigProperties);
        }

        @Override
        public void handle(Endpoint<?> endpoint) {
                HttpServerRequest request = (HttpServerRequest) endpoint.get();

                request.bodyHandler(buffer -> {
                        // message conversion HTTP -> Kafka Producer Record
                        JsonObject body = buffer.toJsonObject();
                        String topic = body.getString("topic");
                        byte[] value = body.getString("message").getBytes();
                        KafkaProducerRecord<String, byte[]> record = KafkaProducerRecord.create(topic,null, value,0);

                        // message settled (by sender), no feedback need by Apache Kafka, no disposition to be sent
                        this.send(record, null);

                        log.info("Message delivered to topic {} with value {}", topic, value);

                        // create the response
                        String message = "Message delivered";
                        sendResponse(request, message, 200);

                });

        }

        public void sendResponse(HttpServerRequest request, String message, int statusCode){
                HttpServerResponse response = request.response();
                response.setChunked(true);
                response.setStatusCode(statusCode);
                response.putHeader("Content-Type", "text/plain");
                response.write(message);
                response.end();
        }
}
