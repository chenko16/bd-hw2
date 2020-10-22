package ru.mephi.chenko.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import ru.mephi.chenko.spark.cassandra.CassandraService;
import ru.mephi.chenko.spark.dto.AggregatedMetricDto;
import ru.mephi.chenko.spark.dto.MetricDto;
import ru.mephi.chenko.spark.kafka.KafkaMetricConsumer;
import ru.mephi.chenko.spark.service.MetricRDDService;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class SparkApplication {

    public static void main(String[] args) {
        if(args.length < 1) {
            System.out.println("Usage: sparkApp minutesAggregateBy...");
            return;
        }

        List<Integer> minuteList = Arrays.asList(args).stream()
                .map(Integer::parseInt)
                .collect(Collectors.toList());

        SparkConf conf = new SparkConf()
                .set("spark.cassandra.connection.host", "127.0.0.1")
                .setAppName("Spark RDD metric aggregator")
                .setMaster("local[*]");

        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        CassandraService cassandraService = new CassandraService(sparkContext);
        KafkaMetricConsumer metricConsumer = new KafkaMetricConsumer(sparkContext);

//        while(true) {

            JavaRDD<MetricDto> inputRdd = metricConsumer.readMetrics();

            cassandraService.writeMetric(inputRdd);

            for(int minuteAggregateBy: minuteList) {
                JavaRDD<MetricDto> metricRdd = cassandraService.readMetrics();

                JavaRDD<AggregatedMetricDto> aggregatedMetricRdd = MetricRDDService.aggregateMetric(metricRdd, minuteAggregateBy);

                cassandraService.writeAggregatedMetric(aggregatedMetricRdd);
            }

//            try {
//                Thread.sleep(5 * 60 * 1000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//
//        }
    }
}
