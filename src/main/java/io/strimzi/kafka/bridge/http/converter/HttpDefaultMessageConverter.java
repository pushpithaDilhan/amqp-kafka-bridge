package io.strimzi.kafka.bridge.http.converter;

import io.strimzi.kafka.bridge.converter.MessageConverter;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.apache.qpid.proton.message.Message;

public class HttpDefaultMessageConverter implements MessageConverter<String, byte[], Message> {
        @Override
        public KafkaProducerRecord<String, byte[]> toKafkaRecord(String kafkaTopic, Message message) {
                return null;
        }

        @Override
        public Message toMessage(String address, KafkaConsumerRecord<String, byte[]> record) {
                return null;
        }
}
