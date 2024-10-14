package dn.rubtsov.parserj_03.processor;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
@Slf4j
public class JsonProducer {
    private final KafkaProducer<String, String> producer;
    private final String topic;

    @PreDestroy
    public void cleanup() {
        close();
    }

    public JsonProducer(@Value("${kafka.topic}") String topic) {
        this.topic = topic;
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:29092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        this.producer = new KafkaProducer<>(properties);
    }

    public void sendMessage(String jsonMessage) {
        producer.send(new ProducerRecord<>(topic, jsonMessage), (metadata, exception) -> {
            if (exception != null) {
                log.error("Ошибка при отправке сообщения в тему {}: {}", topic, exception.getMessage(), exception);
            } else {
                log.info("Сообщение отправлено в topic {} with offset {}", metadata.topic(), metadata.offset());
            }
        });
    }

    public void close() {
        producer.close();
    }
}
