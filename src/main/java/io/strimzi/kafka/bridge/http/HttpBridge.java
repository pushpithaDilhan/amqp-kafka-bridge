package io.strimzi.kafka.bridge.http;

import io.strimzi.kafka.bridge.amqp.AmqpMode;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.http.HttpConnection;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.proton.ProtonConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpBridge extends AbstractVerticle {

    private static final Logger log = LoggerFactory.getLogger(HttpBridge.class);

    private HttpServer server;

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

        this.server = vertx.createHttpServer(options).requestHandler(request -> {
            request.response().end("Hello world");})
                //.connectionHandler(this::processConnection)
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
