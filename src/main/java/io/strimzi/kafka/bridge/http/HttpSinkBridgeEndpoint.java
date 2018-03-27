package io.strimzi.kafka.bridge.http;

import io.strimzi.kafka.bridge.Endpoint;
import io.strimzi.kafka.bridge.SinkBridgeEndpoint;
import io.strimzi.kafka.bridge.config.BridgeConfigProperties;
import io.strimzi.kafka.bridge.config.KafkaConfigProperties;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class HttpSinkBridgeEndpoint<K, V> extends SinkBridgeEndpoint<K, V>{

        private KafkaConsumer<String, byte[]> consumer;
        private KafkaConfigProperties kafkaConfigProperties;

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
                HttpServerRequest request = (HttpServerRequest) endpoint.get();

                // sink bridge endpoint
                String topics_query = request.getParam("topics");

                // subscribe to several topics
                Set<String> topics = new HashSet<String>(Arrays.asList(topics_query.split(" : ")));

                InitConsumer();

                this.consumer.subscribe(topics);

                JsonObject body = new JsonObject();

                this.consumer.handler(record ->{
                        // convert kafka record to HTTP message
                        JsonObject rec = new JsonObject();
                        rec.put("key", record.key());
                        rec.put("offset", record.offset());
                        rec.put("value", new String(record.value()));
                        rec.put("partition", record.partition());
                        body.put("record",rec);
                        Buffer buffer = body.toBuffer();
                        sendResponse(request, buffer);
                });

        }

        public void InitConsumer(){
                this.kafkaConfigProperties = new KafkaConfigProperties();

                Properties consumer_props = new Properties();
                consumer_props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaConfigProperties.getBootstrapServers());
                consumer_props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, this.kafkaConfigProperties.getConsumerConfig().getKeyDeserializer());
                consumer_props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, this.kafkaConfigProperties.getConsumerConfig().getValueDeserializer());
                consumer_props.put(ConsumerConfig.GROUP_ID_CONFIG, "my_group");
                consumer_props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, this.kafkaConfigProperties.getConsumerConfig().isEnableAutoCommit());
                consumer_props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, this.kafkaConfigProperties.getConsumerConfig().getAutoOffsetReset());

                this.consumer = KafkaConsumer.create(vertx, consumer_props);
        }

        public void sendResponse(HttpServerRequest request, Buffer buffer){
                HttpServerResponse response = request.response();
                response.setChunked(true);
                response.write(buffer);
                response.end();
        }
}
