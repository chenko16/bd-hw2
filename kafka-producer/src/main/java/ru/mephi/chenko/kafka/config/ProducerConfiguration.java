package ru.mephi.chenko.kafka.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import ru.mephi.chenko.kafka.dto.MetricDto;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class ProducerConfiguration {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Autowired
    private ObjectMapper objectMapper;


    /**
     * Returns kafka producer properties
     * @return Kafka producer properties
     */
    @Bean
    public Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, KafkaMetricPartitioner.class);

        return props;
    }

    /**
     * Returns kafka producer factory
     * @return Kafka producer factory
     */
    @Bean
    public ProducerFactory<String, MetricDto> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs(), new StringSerializer(), new JsonSerializer<>(objectMapper));
    }

    /**
     * Returns kafka producer template
     * @return Kafka producer template
     */
    @Bean
    public KafkaTemplate<String, MetricDto> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

}
