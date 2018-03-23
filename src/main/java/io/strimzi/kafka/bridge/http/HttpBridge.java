package io.strimzi.kafka.bridge.http;

import io.strimzi.kafka.bridge.config.KafkaConfigProperties;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class HttpBridge extends AbstractVerticle {

        private static final Logger log = LoggerFactory.getLogger(HttpBridge.class);

        private HttpServer server;
        private KafkaConfigProperties kafkaConfigProperties;
        private KafkaProducer<String, byte[]> producer;

        /**
        * Start the Http server
        *
        * @param startFuture
        */
        private void bindHttpServer(Future<Void> startFuture) {

                // log network activity
                HttpServerOptions options = new HttpServerOptions();
                options.setLogActivity(true);
                options.setHost("0.0.0.0");
                options.setPort(9000);

                this.kafkaConfigProperties = new KafkaConfigProperties();
                Properties props = new Properties();
                props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaConfigProperties.getBootstrapServers());
                props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, this.kafkaConfigProperties.getProducerConfig().getKeySerializer());
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, this.kafkaConfigProperties.getProducerConfig().getValueSerializer());
                props.put(ProducerConfig.ACKS_CONFIG, this.kafkaConfigProperties.getProducerConfig().getAcks());

                this.producer = KafkaProducer.create(vertx, props);

                this.server = vertx.createHttpServer(options)
                        .requestHandler(request -> {
                                request.bodyHandler(buffer -> {

                                        // message conversion HTTP -> Kafka Producer Record
                                        JsonObject body = buffer.toJsonObject();
                                        String topic = body.getString("topic");
                                        byte[] value = body.getString("message").getBytes();
                                        KafkaProducerRecord<String, byte[]> record = KafkaProducerRecord.create(topic,null, value,0);

                                        // send to Kafka server
                                        producer.write(record);

                                        log.info("Message delivered to topic {} with value {}", topic, value);
                                });
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
                this.bindHttpServer(startFuture);
        }

}
