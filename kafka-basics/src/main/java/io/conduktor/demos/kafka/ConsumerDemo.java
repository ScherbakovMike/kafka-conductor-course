package io.conduktor.demos.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

@Slf4j
public class ConsumerDemo {
    public static void main(String[] args) {
        var topic = "demo_java";
        var properties = kafkaProperties();

        //create a consumer
        try (var consumer = new KafkaConsumer<>(properties)) {
            //subscribe to a topic
            consumer.subscribe(List.of(topic));
            // poll for data
            while (true) {
                log.info("Polling");
                var records = consumer.poll(Duration.ofMillis(1000));
                records.forEach(recordItem -> {
                    log.info("Key: " + recordItem.key()+ ", value: "+recordItem.value());
                    log.info("Partition: " + recordItem.partition()+ ", offset: "+recordItem.offset());
                });
            }
        }
    }

    private static Properties kafkaProperties() {

        var groupId = "my-java-application";
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
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");
        return properties;
    }
}
