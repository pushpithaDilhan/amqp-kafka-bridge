package io.strimzi.kafka.bridge.http;

import io.strimzi.kafka.bridge.Endpoint;
import io.strimzi.kafka.bridge.SourceBridgeEndpoint;
import io.strimzi.kafka.bridge.config.BridgeConfigProperties;
import io.strimzi.kafka.bridge.config.KafkaConfigProperties;
import io.strimzi.kafka.bridge.converter.MessageConverter;
import io.strimzi.kafka.bridge.http.converter.HttpDefaultMessageConverter;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class HttpSourceBridgeEndpoint extends SourceBridgeEndpoint{

        private KafkaProducer<String, byte[]> producer;
        private KafkaConfigProperties kafkaConfigProperties;

        // converter from HTTP message to ProducerRecord
        private MessageConverter<String, byte[], Buffer> converter;

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

                InitProducer();

                if (this.converter == null) {
                        this.converter = new HttpDefaultMessageConverter();
                }

                request.bodyHandler(buffer -> {
                        // message conversion HTTP -> Kafka Producer Record
                        JsonObject body = buffer.toJsonObject();
                        String topic = body.getString("topic");

                        KafkaProducerRecord<String, byte[]> record = this.converter.toKafkaRecord(topic, buffer);

                        // message settled (by sender), no feedback need by Apache Kafka, no disposition to be sent
                        this.producer.write(record, null);

                        log.info("Message delivered to topic {}", topic);

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

        public void InitProducer(){
                this.kafkaConfigProperties = new KafkaConfigProperties();
                Properties props = new Properties();
                props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaConfigProperties.getBootstrapServers());
                props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, this.kafkaConfigProperties.getProducerConfig().getKeySerializer());
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, this.kafkaConfigProperties.getProducerConfig().getValueSerializer());
                props.put(ProducerConfig.ACKS_CONFIG, this.kafkaConfigProperties.getProducerConfig().getAcks());
                this.producer = KafkaProducer.create(vertx, props);
        }
}
