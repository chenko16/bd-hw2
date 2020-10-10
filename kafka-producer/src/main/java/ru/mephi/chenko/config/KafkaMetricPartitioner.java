package ru.mephi.chenko.config;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import ru.mephi.chenko.dto.MetricDto;

import java.util.List;
import java.util.Map;

@Component
public class KafkaMetricPartitioner implements Partitioner {
    private static Logger log = LoggerFactory.getLogger(KafkaMetricPartitioner.class);

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List partitionInfos = cluster.partitionsForTopic(topic);
        int numberOfPartitions = partitionInfos.size();
        MetricDto metric = (MetricDto) value;

        return metric.getId().intValue() & Integer.MAX_VALUE % numberOfPartitions;
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map configs) {
    }

    @Override
    public void onNewBatch(String topic, Cluster cluster, int prevPartition) {
    }
}
