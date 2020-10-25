package ru.mephi.chenko.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import ru.mephi.chenko.spark.cassandra.CassandraService;
import ru.mephi.chenko.spark.dto.AggregatedMetricDto;
import ru.mephi.chenko.spark.dto.MetricDto;
import ru.mephi.chenko.spark.kafka.KafkaMetricConsumer;
import ru.mephi.chenko.spark.service.MetricRDDService;
import ru.mephi.chenko.spark.util.DateUtil;

import java.util.Arrays;
import java.util.List;

public class SparkApplication {

    public static void main(String[] args) throws IllegalAccessException {
        if(args.length < 1) {
            System.out.println("Usage: sparkApp minutesAggregateBy...");
            return;
        }

        List<String> scaleList = Arrays.asList(args);
        for(String scale: scaleList) {
            DateUtil.validateScale(scale);
        }

        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .set("spark.cassandra.connection.host", "127.0.0.1")
                .setAppName("Spark RDD metric aggregator");

        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        CassandraService cassandraService = new CassandraService(sparkContext);
        KafkaMetricConsumer metricConsumer = new KafkaMetricConsumer(sparkContext);

        JavaRDD<MetricDto> inputRdd = metricConsumer.readMetrics();

        cassandraService.writeMetric(inputRdd);

        for(String scale: scaleList) {
            JavaRDD<MetricDto> metricRdd = cassandraService.readMetrics();

            JavaRDD<AggregatedMetricDto> aggregatedMetricRdd = MetricRDDService.aggregateMetric(metricRdd, scale);

            cassandraService.writeAggregatedMetric(aggregatedMetricRdd);
        }
    }
}
