package ru.mephi.chenko.spark.service;

import org.apache.spark.api.java.JavaRDD;
import ru.mephi.chenko.spark.dto.AggregatedMetricDto;
import ru.mephi.chenko.spark.dto.MetricDto;
import ru.mephi.chenko.spark.util.DateUtil;
import scala.Tuple2;

public class MetricRDDService {

    /**
     * Aggregate metrics by id and time
     * @param rdd Source metrics
     * @return Aggregated metrics
     */
    public static JavaRDD<AggregatedMetricDto> aggregateMetric(JavaRDD<MetricDto> rdd, String scale) {
        return rdd
                .map(metric -> {
                    metric.setTime(DateUtil.round(metric.getTime(), scale));
                    return metric;
                })
                .mapToPair(metric -> new Tuple2<>(new Tuple2<>(metric.getId(), metric.getTime()), new Tuple2<>(metric.getValue(), 1)))
                .reduceByKey((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
                .mapValues(value -> value._1 / value._2)
                .map(pair -> new AggregatedMetricDto(pair._1._1, pair._1._2, scale, pair._2));
    }
}
