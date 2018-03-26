package io.strimzi.kafka.bridge.http;

import io.strimzi.kafka.bridge.config.KafkaConfigProperties;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class HttpBridge extends AbstractVerticle {

        private static final Logger log = LoggerFactory.getLogger(HttpBridge.class);

        private HttpServer server;
        private KafkaConfigProperties kafkaConfigProperties;
        private KafkaProducer<String, byte[]> producer;
        private KafkaConsumer<String, byte[]> consumer;

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

                Properties producer_props = new Properties();
                producer_props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaConfigProperties.getBootstrapServers());
                producer_props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, this.kafkaConfigProperties.getProducerConfig().getKeySerializer());
                producer_props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, this.kafkaConfigProperties.getProducerConfig().getValueSerializer());
                producer_props.put(ProducerConfig.ACKS_CONFIG, this.kafkaConfigProperties.getProducerConfig().getAcks());

                Properties consumer_props = new Properties();
                consumer_props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaConfigProperties.getBootstrapServers());
                consumer_props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, this.kafkaConfigProperties.getConsumerConfig().getKeyDeserializer());
                consumer_props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, this.kafkaConfigProperties.getConsumerConfig().getValueDeserializer());
                consumer_props.put(ConsumerConfig.GROUP_ID_CONFIG, "my_group");
                consumer_props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, this.kafkaConfigProperties.getConsumerConfig().isEnableAutoCommit());
                consumer_props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, this.kafkaConfigProperties.getConsumerConfig().getAutoOffsetReset());

                this.consumer = KafkaConsumer.create(vertx, consumer_props);
                this.producer = KafkaProducer.create(vertx, producer_props);

                this.server = vertx.createHttpServer(options)
                        .requestHandler(request -> {
                                if(request.method() == HttpMethod.POST){
                                        request.bodyHandler(buffer -> {

                                                // message conversion HTTP -> Kafka Producer Record
                                                JsonObject body = buffer.toJsonObject();
                                                String topic = body.getString("topic");
                                                byte[] value = body.getString("message").getBytes();
                                                KafkaProducerRecord<String, byte[]> record = KafkaProducerRecord.create(topic,null, value,0);

                                                // send to Kafka server
                                                producer.write(record);

                                                log.info("Message delivered to topic {} with value {}", topic, value);

                                                // create the response
                                                HttpServerResponse response = request.response();
                                                response.setChunked(true);
                                                response.setStatusCode(200);
                                                response.putHeader("Content-Type", "text/plain");
                                                response.write("Message delivered to topic "+ topic +" with value " + value);
                                                response.end();
                                        });
                                }

                                if(request.method() == HttpMethod.GET){
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
                                        // handle request - status 405 - method not allowed
                                }
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
