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
import java.util.concurrent.TimeUnit;

@Slf4j
public class ProducerDemoKeys {

    public static void main(String[] args) {

        // create producer properties
        var properties = producerProperties();

        // create the producer
        try (var producer = new KafkaProducer<String, String>(properties)) {

            for (int j = 0; j < 2; j++) {
                for (var i = 0; i < 10; i++) {
                    String topic = "demo_java";
                    String key = "id " + i;
                    String value = "Hello world " + i;

                    // create a producer record
                    var producerRecord = new ProducerRecord<>(topic, key, value);

                    // send data
                    producer.send(producerRecord, (metadata, e) -> {
                        // executes every time a record successfully sent or an exception is thrown
                        if (Objects.isNull(e)) {
                            // the record was successfully sent
                            log.info("Key: " + key + " | Partition: " + metadata.partition());
                        } else {
                            log.error("Error while producing", e);
                        }
                    });
                }
                TimeUnit.MILLISECONDS.sleep(500);
            }
            // tell the producer to send all data and block until done -- synchronous
            producer.flush();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static Properties producerProperties() {
        Properties properties = null;
        var resourcesPath = Path.of(ProducerDemoKeys.class.getClassLoader().getResource("properties.config").getPath());
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
