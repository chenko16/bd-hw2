package ru.mephi.chenko.kafka.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import ru.mephi.chenko.kafka.dto.MetricDto;


@Service
public class KafkaSender {

    private static final Logger logger = LoggerFactory.getLogger(KafkaSender.class);

    @Value("${kafka.topic}")
    private String topic;

    @Autowired
    private KafkaTemplate<String, MetricDto> kafkaTemplate;

    /**
     * Send metric to kafka
     * @param metric Metric for sending to kafka
     * @return
     */
    public ListenableFuture<SendResult<String, MetricDto>> send(MetricDto metric) {
        logger.info("sending testDataDto='{}'", metric.toString());
        return kafkaTemplate.send(topic, metric);
    }
}
