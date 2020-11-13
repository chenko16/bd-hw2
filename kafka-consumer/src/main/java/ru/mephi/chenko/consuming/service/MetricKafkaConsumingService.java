package ru.mephi.chenko.consuming.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import ru.mephi.chenko.consuming.dao.Metric;
import ru.mephi.chenko.consuming.dao.MetricRepository;
import ru.mephi.chenko.consuming.dto.MetricDto;

@Service
public class MetricKafkaConsumingService {

    @Autowired
    private MetricRepository metricRepository;

    /**
     * Read record from kafka and save to cassandra
     * @param dto dto of metric record
     * @return Metric
     */
    @KafkaListener(topics = {"#{@topicName}"}, containerFactory = "metricKafkaListenerContainerFactory")
    public void consume(MetricDto dto) {
        Metric metric = new Metric(dto.getId(), dto.getTime(), dto.getValue());
        metricRepository.save(metric);
    }
}
