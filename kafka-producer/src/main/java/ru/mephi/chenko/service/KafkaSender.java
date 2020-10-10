package ru.mephi.chenko.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ru.mephi.chenko.dto.MetricDto;


@Service
public class KafkaSender {

    private static final Logger logger = LoggerFactory.getLogger(KafkaSender.class);

    @Value("${kafka.topic}")
    private String jsonTopic;

    @Autowired
    private KafkaTemplate<String, MetricDto> kafkaTemplate;

    public void send(MetricDto testDataDto) {
        logger.info("sending testDataDto='{}'", testDataDto.toString());
        kafkaTemplate.send(jsonTopic, testDataDto);
    }
}
