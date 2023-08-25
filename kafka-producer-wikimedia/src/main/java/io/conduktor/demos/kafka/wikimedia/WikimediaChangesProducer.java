package io.conduktor.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Slf4j
public class WikimediaChangesProducer {

    public static void main(String[] args) throws InterruptedException {
        var topic = "wikimedia.recentchange";

        var properties = producerProperties();
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        var producer = new KafkaProducer<String, String>(properties);

        var eventHandler = new WikimediaChangeHandler(producer, topic);
        var url = "https://stream.wikimedia.org/v2/stream/recentchange";
        var eventSource = new BackgroundEventSource.Builder(
                eventHandler,
                new EventSource.Builder(URI.create(url))
        ).build();
        eventSource.start();

        TimeUnit.MINUTES.sleep(10);
        eventSource.close();
    }

    private static Properties producerProperties() {
        Properties properties = null;
        var resourcesPath = Path.of(WikimediaChangesProducer.class.getClassLoader().getResource("properties.config").getPath());
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
