package io.strimzi.kafka.bridge.http;

import io.strimzi.kafka.bridge.Endpoint;
import io.vertx.core.http.HttpServerRequest;

public class HttpEndpoint implements Endpoint<HttpServerRequest> {
        private HttpServerRequest httpServerRequest;

        /**
         * Contructor
         *
         * @param httpServerRequest  HttpServerRequest representing the remote endpoint
         */
        public HttpEndpoint(HttpServerRequest httpServerRequest) {
                this.httpServerRequest = httpServerRequest;
        }

        @Override
        public HttpServerRequest get() {
                return this.httpServerRequest;
        }
}
