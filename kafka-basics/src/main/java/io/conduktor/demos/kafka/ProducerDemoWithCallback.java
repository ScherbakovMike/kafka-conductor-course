package io.conduktor.demos.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Properties;

@Slf4j
public class ProducerDemoWithCallback {

    public static void main(String[] args) {

        // create producer properties
        var properties = producerProperties();

        // create the producer
        try (var producer = new KafkaProducer<String, String>(properties)) {
            // create a producer record
            var producerRecord = new ProducerRecord<String, String>("demo_java", "hello world");

            // send data
            producer.send(producerRecord, (metadata, e) -> {
                // executes every time a record successfully sent or an exception is thrown
                if (Objects.isNull(e)) {
                    // the record was successfully sent
                    log.info("Received new metadata \n" +
                            "Topic: " + metadata.topic() + "\n" +
                            "Partition: " + metadata.partition() + "\n" +
                            "Offset: " + metadata.offset() + "\n" +
                            "Timestamp: " + metadata.timestamp());
                } else {
                    log.error("Error while producing", e);
                }
            });

            // tell the producer to send all data and block until done -- synchronous
            producer.flush();
        }
    }

    private static Properties producerProperties() {
        Properties properties = null;
        var resourcesPath = Path.of(ProducerDemoWithCallback.class.getClassLoader().getResource("properties.config").getPath());
        try {
            var propertiesIOS = Files.newInputStream(resourcesPath);
            properties = new Properties();
            properties.load(propertiesIOS);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            System.exit(1);
        }
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        return properties;
    }
}
