package ru.mephi.chenko.consuming.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import ru.mephi.chenko.consuming.dto.MetricDto;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
public class KafkaConsumerConfig {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.group-id}")
    private String groupId;

    @Value("${kafka.topic}")
    private String topic;

    @Autowired
    private ObjectMapper objectMapper;

    /**
     * Returns kafka consumer properties
     * @return Kafka consumer properties
     */    @Bean
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        return props;
    }

    /**
     * Returns metric json deserializer
     * @return metric json deserializer
     */
    @Bean
    public JsonDeserializer<MetricDto> metricJsonDeserializer() {
        JsonDeserializer<MetricDto> deserializer = new JsonDeserializer<>(MetricDto.class);
        deserializer.setRemoveTypeHeaders(false);
        deserializer.addTrustedPackages("*");
        deserializer.setUseTypeMapperForKey(true);

        return deserializer;
    }


    /**
     * Returns kafka consumer factory
     * @return kafka consumer factory
     */
    @Bean
    public ConsumerFactory<String, MetricDto> metricConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs(), new StringDeserializer(), metricJsonDeserializer());
    }

    /**
     * Returns kafka consumer listener container factory
     * @return kafka consumer listener container factory
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, MetricDto> metricKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, MetricDto> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(metricConsumerFactory());
        return factory;
    }

    /**
     * Returns kafka topic name
     * @return kafka topic name
     */
    @Bean
    public String topicName() {
        return topic;
    }
}
