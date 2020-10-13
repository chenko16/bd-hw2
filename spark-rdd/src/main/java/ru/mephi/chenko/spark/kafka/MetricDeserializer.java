package ru.mephi.chenko.spark.kafka;

import org.apache.kafka.common.serialization.Deserializer;
import org.codehaus.jackson.map.ObjectMapper;
import ru.mephi.chenko.spark.dto.MetricDto;

import java.io.IOException;

public class MetricDeserializer implements Deserializer<MetricDto> {

    /**
     * Deserialize value of kafka value to MetricDto
     * @param s Name of the kafka topic
     * @param bytes kafka record values bytes
     * @return Metric
     */
    @Override
    public MetricDto deserialize(String s, byte[] bytes) {
        ObjectMapper objectMapper = new ObjectMapper();
        MetricDto metricDto = null;

        try {
            metricDto = objectMapper.readValue(bytes, MetricDto.class);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return metricDto;
    }

}
