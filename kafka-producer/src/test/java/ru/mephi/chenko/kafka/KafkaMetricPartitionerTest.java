package ru.mephi.chenko.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import ru.mephi.chenko.kafka.config.KafkaMetricPartitioner;
import ru.mephi.chenko.kafka.dto.MetricDto;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@RunWith(SpringRunner.class)
@ActiveProfiles("test")
public class KafkaMetricPartitionerTest {

    @Value("kafka.topic")
    private String topic;

    @Autowired
    private KafkaMetricPartitioner metricPartitioner;

    @Test
    public void testKafkaPartitioner() throws JsonProcessingException {
        PartitionInfo partitionInfo0 = new PartitionInfo(topic, 0, null, null, null);
        PartitionInfo partitionInfo1 = new PartitionInfo(topic, 1, null, null, null);
        List<PartitionInfo> partitionList = new ArrayList<>();
        partitionList.add(partitionInfo0);
        partitionList.add(partitionInfo1);

        Cluster cluster = new Cluster("kafka_test", new ArrayList<Node>(), partitionList, new HashSet<>(), new HashSet<>());

        ObjectMapper objectMapper = new ObjectMapper();

        MetricDto firstMetric = new MetricDto(1L, new Date(), 0);
        MetricDto secondMetric = new MetricDto(2L, new Date(), 0);

        String firstMetricJson = objectMapper.writeValueAsString(firstMetric);
        String secondMetricJson = objectMapper.writeValueAsString(secondMetric);

        int firstMetricPartition = metricPartitioner.partition(topic, 1, "1".getBytes(), firstMetric, firstMetricJson.getBytes(), cluster);
        int secondMetricPartition = metricPartitioner.partition(topic, 2, "2".getBytes(), secondMetric, secondMetricJson.getBytes(), cluster);

        assertEquals(1, firstMetricPartition);
        assertEquals(0, secondMetricPartition);
    }
}
