package io.strimzi.kafka.bridge.http.converter;

import io.strimzi.kafka.bridge.converter.MessageConverter;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaProducerRecord;

public class HttpDefaultMessageConverter implements MessageConverter<String, byte[], Buffer> {
        @Override
        public KafkaProducerRecord<String, byte[]> toKafkaRecord(String kafkaTopic, Buffer message) {
                JsonObject body = message.toJsonObject();
                byte[] value = body.getString("message").getBytes();
                KafkaProducerRecord<String, byte[]> record = KafkaProducerRecord.create(kafkaTopic,null, value,0);
                return record;
        }

        @Override
        public Buffer toMessage(String address, KafkaConsumerRecord<String, byte[]> record) {
                JsonObject rec = new JsonObject();
                rec.put("key", record.key());
                rec.put("offset", record.offset());
                rec.put("value", new String(record.value()));
                rec.put("partition", record.partition());
                Buffer buffer = rec.toBuffer();
                return buffer;
        }
}
