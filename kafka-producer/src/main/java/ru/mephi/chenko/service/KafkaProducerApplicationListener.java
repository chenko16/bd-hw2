package ru.mephi.chenko.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Service;
import ru.mephi.chenko.dto.MetricDto;

import java.io.File;
import java.util.List;

@Service
public class KafkaProducerApplicationListener implements ApplicationListener<ContextRefreshedEvent> {

    @Autowired
    private CVSService cvsService;

    @Autowired
    private KafkaSender kafkaSender;

    @Override
    public void onApplicationEvent(ContextRefreshedEvent contextRefreshedEvent) {
        File csvFile = new File("log.csv");
        List<MetricDto> metricList = cvsService.parseCsv(csvFile);
        metricList.forEach(metric -> kafkaSender.send(metric));
    }
}
