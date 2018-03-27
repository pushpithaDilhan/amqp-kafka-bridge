package io.strimzi.kafka.bridge.http;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.http.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

public class    HttpBridge extends AbstractVerticle {

        private static final Logger log = LoggerFactory.getLogger(HttpBridge.class);

        private HttpServer server;
        private HttpBridgeConfigProperties bridgeConfigProperties;


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
                if (mode.equals(HttpMode.SERVER)) {
                        this.bindHttpServer(startFuture);
                }else{
                        // when bridge acts as a client
                }
        }

        @Override
        public void stop(Future<Void> stopFuture) throws Exception {

                log.info("Stopping HTTP-Kafka bridge verticle ...");

                // close related connections and others

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
                        new HttpSinkBridgeEndpoint<>(this.vertx, this.bridgeConfigProperties).handle(new HttpEndpoint(request));
                }
                else{
                        // 405 - method not allowed
                }
        }

}
