package ru.mephi.chenko.spark.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;
import ru.mephi.chenko.spark.dto.MetricDto;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaMetricConsumer {

    private final JavaSparkContext sparkContext;
    private final Map<String, Object> kafkaParams = new HashMap<>();
    private final List<OffsetRange> offsetRangesList = new ArrayList<>();

    private static final String TOPICS = "hw2-source";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    /**
     * KafkaMetricConsumer all arguments constructor
     * @param sparkContext Context of job
     * @return KafkaMetricConsumer
     */
    public KafkaMetricConsumer(JavaSparkContext sparkContext) {
        this.sparkContext = sparkContext;

        kafkaParams.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", MetricDeserializer.class);
        kafkaParams.put("group.id", "id");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        // topic, partition, inclusive starting offset, exclusive ending offset
        offsetRangesList.add(OffsetRange.create(TOPICS, 0, 0, 10000));
        offsetRangesList.add(OffsetRange.create(TOPICS, 1, 0, 10000));
    }


    /**
     * Read metrics from kafka
     * @return JavaRDD of metrics
     */
    public JavaRDD<MetricDto> readMetrics() {
        OffsetRange[] offsetRanges = new OffsetRange[offsetRangesList.size()];
        offsetRangesList.toArray(offsetRanges);

        JavaRDD<ConsumerRecord<String, MetricDto>> rdd = KafkaUtils.createRDD(
                sparkContext,
                kafkaParams,
                offsetRanges,
                LocationStrategies.PreferConsistent()
        );

        return rdd.map(ConsumerRecord::value);
    }
}
