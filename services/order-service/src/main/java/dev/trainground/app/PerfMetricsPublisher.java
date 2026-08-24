package dev.trainground.app;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
class PerfMetricsPublisher {
    private final KafkaProducer<String, String> producer;

    PerfMetricsPublisher(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("acks", "0");
        this.producer = new KafkaProducer<>(props);
    }

    void publish(String operation, long durationNanos, String status) {
        String threadName = Thread.currentThread().getName();
        String json = String.format(
                "{\"operation\":\"%s\",\"durationNanos\":%d,\"threadName\":\"%s\",\"status\":\"%s\",\"timestamp\":%d}",
                operation, durationNanos, threadName, status, System.currentTimeMillis()
        );
        try {
            producer.send(new ProducerRecord<>("perf-metrics", threadName, json));
        } catch (Exception e) {
            // best effort - не даём диагностике сломать основной поток
        }
    }
}
