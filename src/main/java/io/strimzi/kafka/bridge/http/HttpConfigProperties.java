package io.strimzi.kafka.bridge.http;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * HTTP configuration properties
 */
@Component
@ConfigurationProperties(prefix = "http")
public class HttpConfigProperties {

        private static final HttpMode DEFAULT_AMQP_MODE = HttpMode.SERVER;
        private static final String DEFAULT_HOST = "0.0.0.0";
        private static final int DEFAULT_PORT = 9000;
        private static final String DEFAULT_MESSAGE_CONVERTER = "io.strimzi.kafka.bridge.http.converter.AmqpDefaultMessageConverter";
        private static final String DEFAULT_CERT_DIR = null;

        private HttpMode mode = DEFAULT_AMQP_MODE;
        private String host = DEFAULT_HOST;
        private int port = DEFAULT_PORT;
        private String messageConverter = DEFAULT_MESSAGE_CONVERTER;
        private String certDir = DEFAULT_CERT_DIR;

        /**
         * Get the bridge working mode (client or server)
         *
         * @return
         */
        public HttpMode getMode() {
                return this.mode;
        }

        /**
         * Set the bridge working mode
         *
         * @param mode  bridge working mode
         * @return  this instance for setter chaining
         */
        public HttpConfigProperties setMode(HttpMode mode) {
                this.mode = mode;
                return this;
        }

        /**
         * Get the host for HTTP client (to connect) or server (to bind)
         *
         * @return
         */
        public String getHost() {
                return this.host;
        }

        /**
         * Set the host for HTTP client (to connect) or server (to bind)
         *
         * @param host  HTTP host
         * @return  this instance for setter chaining
         */
        public HttpConfigProperties setHost(String host) {
                this.host = host;
                return this;
        }

        /**
         * Get the port for HTTP client (to connect) or server (to bind)
         *
         * @return
         */
        public int getPort() {
                return this.port;
        }

        /**
         * Set the port for HTTP client (to connect) or server (to bind)
         *
         * @param port  HTTP port
         * @return  this instance for setter chaining
         */
        public void setPort(int port) {
                this.port = port;
        }

        /**
         * Get the HTTP message converter
         *
         * @return
         */
        public String getMessageConverter() {
                return this.messageConverter;
        }

        /**
         * Set the HTTP message converter
         *
         * @param messageConverter  AMQP message converter
         * @return  this instance for setter chaining
         */
        public HttpConfigProperties setMessageConverter(String messageConverter) {
                this.messageConverter = messageConverter;
                return this;
        }

        /**
         * Get the directory with the TLS certificates files
         *
         * @return
         */
        public String getCertDir() {
                return this.certDir;
        }

        /**
         * Set the directory with the TLS certificates files
         *
         * @param certDir  Path to the TLS certificate files
         * @return  this instance for setter chaining
         */
        public HttpConfigProperties setCertDir(String certDir) {
                this.certDir = certDir;
                return this;
        }
}
