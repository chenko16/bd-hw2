package ru.mephi.chenko.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.mephi.chenko.spark.dto.AggregatedMetricDto;
import ru.mephi.chenko.spark.dto.MetricDto;
import ru.mephi.chenko.spark.service.MetricRDDService;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SparkRDDTest {

    private JavaSparkContext sparkContext;

    private final List<MetricDto> input = new ArrayList<>();
    private final List<AggregatedMetricDto> expectedOutput = new ArrayList<>();

    @BeforeEach
    public void init() throws ParseException {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[2]");
        conf.setAppName("junit");
        sparkContext = new JavaSparkContext(conf);

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        input.add(new MetricDto(1L, dateFormat.parse("2020-10-12 12:00:25"), 10));
        input.add(new MetricDto(1L, dateFormat.parse("2020-10-12 12:02:25"), 30));
        input.add(new MetricDto(1L, dateFormat.parse("2020-10-12 12:04:25"), 20));
        input.add(new MetricDto(1L, dateFormat.parse("2020-10-12 12:06:25"), 30));
        input.add(new MetricDto(1L, dateFormat.parse("2020-10-12 12:08:25"), 50));
        input.add(new MetricDto(1L, dateFormat.parse("2020-10-12 12:10:25"), 10));
        input.add(new MetricDto(1L, dateFormat.parse("2020-10-13 12:12:25"), 90));
        input.add(new MetricDto(2L, dateFormat.parse("2020-10-12 12:00:25"), 60));

        expectedOutput.add(new AggregatedMetricDto(1L, dateFormat.parse("2020-10-12 12:00:00"), "5m", 20));
        expectedOutput.add(new AggregatedMetricDto(1L, dateFormat.parse("2020-10-12 12:05:00"), "5m", 40));
        expectedOutput.add(new AggregatedMetricDto(1L, dateFormat.parse("2020-10-12 12:10:00"), "5m", 10));
        expectedOutput.add(new AggregatedMetricDto(1L, dateFormat.parse("2020-10-13 12:10:00"), "5m", 90));
        expectedOutput.add(new AggregatedMetricDto(2L, dateFormat.parse("2020-10-12 12:00:00"), "5m", 60));
    }

    @Test
    public void testMetricAggregation() {
        JavaRDD<MetricDto> metricRdd = sparkContext.parallelize(input, 2);
        JavaRDD<AggregatedMetricDto> resultRdd = MetricRDDService.aggregateMetric(metricRdd, "5m");
        List<AggregatedMetricDto> resultList = resultRdd.collect();
        assertEquals(expectedOutput.size(), resultList.size());
        assertTrue(resultList.containsAll(expectedOutput));
    }
}