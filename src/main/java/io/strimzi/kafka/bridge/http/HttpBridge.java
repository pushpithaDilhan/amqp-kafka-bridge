package io.strimzi.kafka.bridge.http;

import io.strimzi.kafka.bridge.config.KafkaConfigProperties;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.*;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.producer.KafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.*;

public class    HttpBridge extends AbstractVerticle {

        private static final Logger log = LoggerFactory.getLogger(HttpBridge.class);

        private HttpServer server;
        private HttpBridgeConfigProperties bridgeConfigProperties;
        private KafkaConfigProperties kafkaConfigProperties;
        private KafkaProducer<String, byte[]> producer;
        private KafkaConsumer<String, byte[]> consumer;

        @Autowired
        public void setBridgeConfigProperties(HttpBridgeConfigProperties bridgeConfigProperties) {
                this.bridgeConfigProperties = bridgeConfigProperties;
        }

        /**
        * Start the Http server
        *
        * @param startFuture
        */
        private void bindHttpServer(Future<Void> startFuture) {

                this.kafkaConfigProperties = new KafkaConfigProperties();

                Properties consumer_props = new Properties();
                consumer_props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaConfigProperties.getBootstrapServers());
                consumer_props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, this.kafkaConfigProperties.getConsumerConfig().getKeyDeserializer());
                consumer_props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, this.kafkaConfigProperties.getConsumerConfig().getValueDeserializer());
                consumer_props.put(ConsumerConfig.GROUP_ID_CONFIG, "my_group");
                consumer_props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, this.kafkaConfigProperties.getConsumerConfig().isEnableAutoCommit());
                consumer_props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, this.kafkaConfigProperties.getConsumerConfig().getAutoOffsetReset());

                this.consumer = KafkaConsumer.create(vertx, consumer_props);

                this.server = vertx.createHttpServer(getServerOptions())
                        .requestHandler(request -> {
                                processRequest(request);
                        })
                        .listen(ar -> {
                                if (ar.succeeded()) {
                                        log.info("HTTP-Kafka Bridge started and listening on port {}", ar.result().actualPort());
                                        startFuture.complete();
                                } else {
                                        log.error("Error starting HTTP-Kafka Bridge", ar.cause());
                                        startFuture.fail(ar.cause());
                                }
                        });
        }

        @Override
        public void start(Future<Void> startFuture) throws Exception {

                log.info("Starting HTTP-Kafka bridge verticle...");

                HttpMode mode = this.bridgeConfigProperties.getEndpointConfigProperties().getMode();
                log.info("HTTP-Kafka Bridge configured in {} mode", mode);
                if (mode == HttpMode.SERVER) {
                        this.bindHttpServer(startFuture);
                }else{
                        // when bridge acts as a client
                }
        }

        @Override
        public void stop(Future<Void> stopFuture) throws Exception {

                log.info("Stopping HTTP-Kafka bridge verticle ...");

                if (this.server != null) {

                        this.server.close(done -> {

                                if (done.succeeded()) {
                                        log.info("HTTP-Kafka bridge has been shut down successfully");
                                        stopFuture.complete();
                                } else {
                                        log.info("Error while shutting down HTTP-Kafka bridge", done.cause());
                                        stopFuture.fail(done.cause());
                                }
                        });
                }
        }

        /**
         * Create an options instance for the HttpServer
         * based on HTTP-Kafka bridge internal configuration
         *
         * @return		HttpServer options instance
         */
        public HttpServerOptions getServerOptions(){
                HttpServerOptions options = new HttpServerOptions();
                // log network activity
                options.setLogActivity(true);
                options.setHost(this.bridgeConfigProperties.getEndpointConfigProperties().getHost());
                options.setPort(this.bridgeConfigProperties.getEndpointConfigProperties().getPort());
                return options;
        }

        /**
         * Process HTTP request
         * based on HTTP request method
         */
        public void processRequest(HttpServerRequest request){
                if(request.method().equals(HttpMethod.POST)){
                        new HttpSourceBridgeEndpoint(this.vertx, this.bridgeConfigProperties).handle(new HttpEndpoint(request));
                }
                if(request.method().equals(HttpMethod.GET)){
                        // sink bridge endpoint
                        String topics_query = request.getParam("topics");

                        // subscribe to several topics
                        Set<String> topics = new HashSet<String>(Arrays.asList(topics_query.split(" : ")));
                        consumer.subscribe(topics);

                        JsonObject body = new JsonObject();

                        consumer.handler(record ->{
                                JsonObject rec = new JsonObject();
                                rec.put("key", record.key());
                                rec.put("offset", record.offset());
                                rec.put("value", new String(record.value()));
                                rec.put("partition", record.partition());
                                body.put("record",rec);
                                Buffer buffer = body.toBuffer();
                                HttpServerResponse response = request.response();
                                response.setChunked(true);
                                response.write(buffer);
                                response.end();
                        });
                }
                else{
                        // 405 - method not allowed
                }
        }

}
