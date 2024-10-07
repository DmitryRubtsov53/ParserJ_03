package dn.rubtsov.parserj_03.processor;

import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
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
                exception.printStackTrace();
            } else {
                System.out.printf("Sent message to topic %s with offset %d%n", metadata.topic(), metadata.offset());
            }
        });
    }

    public void close() {
        producer.close();
    }
}
