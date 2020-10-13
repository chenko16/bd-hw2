package ru.mephi.chenko.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import ru.mephi.chenko.spark.cassandra.CassandraMetricWriter;
import ru.mephi.chenko.spark.dto.AggregatedMetricDto;
import ru.mephi.chenko.spark.dto.MetricDto;
import ru.mephi.chenko.spark.kafka.KafkaMetricConsumer;
import ru.mephi.chenko.spark.service.MetricRDDService;

public class SparkApplication {

    public static void main(String[] args) {
        if(args.length != 1) {
            System.out.println("Usage: sparkApp minutesAggregateBy");
            return;
        }

        int minutes = Integer.parseInt(args[0]);

        SparkConf conf = new SparkConf()
                .set("spark.cassandra.connection.host", "127.0.0.1")
                .setAppName("Spark RDD metric aggregator")
                .setMaster("local[2]");

        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        KafkaMetricConsumer metricConsumer = new KafkaMetricConsumer(sparkContext);
        JavaRDD<MetricDto> metricRdd = metricConsumer.readMetrics();

        JavaRDD<AggregatedMetricDto> aggregatedMetricRdd = MetricRDDService.aggregateMetric(metricRdd, minutes);

        CassandraMetricWriter metricWriter = new CassandraMetricWriter(sparkContext);
        metricWriter.writeMetric(aggregatedMetricRdd);
    }
}
