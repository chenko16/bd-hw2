package ru.mephi.chenko.kafka.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Service;
import ru.mephi.chenko.kafka.dto.MetricDto;

import java.io.File;
import java.util.List;

@Service
@Profile("!test")
public class KafkaProducerApplicationListener implements ApplicationListener<ContextRefreshedEvent> {

    @Autowired
    private CSVService csvService;

    @Autowired
    private KafkaSender kafkaSender;

    /**
     * Parse source csv and send metrics to kafka after application context has been initialized
     * @param contextRefreshedEvent Event of initializing application context
     */
    @Override
    public void onApplicationEvent(ContextRefreshedEvent contextRefreshedEvent) {
        File csvFile = new File("log.csv");
        List<MetricDto> metricList = csvService.parseCsv(csvFile);
        metricList.forEach(metric -> kafkaSender.send(metric));
    }
}
