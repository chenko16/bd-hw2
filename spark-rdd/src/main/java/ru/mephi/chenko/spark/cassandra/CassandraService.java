package ru.mephi.chenko.spark.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import ru.mephi.chenko.spark.dto.AggregatedMetricDto;
import ru.mephi.chenko.spark.dto.MetricDto;

public class CassandraService {

    private final JavaSparkContext sparkContext;

    /**
     * CassandraMetricWriter all arguments constructor
     * @param sparkContext Context of job
     * @return KafkaMetricConsumer
     */
    public CassandraService(JavaSparkContext sparkContext) {
        this.sparkContext = sparkContext;

        //Создаю коннектор к  Cassandra для native запроса
        CassandraConnector connector = CassandraConnector.apply(sparkContext.getConf());

        try (CqlSession session = connector.openSession()) {
            //Создаю таблицу для входных данных
            session.execute("CREATE TABLE IF NOT EXISTS hw2.metric (id int, time timestamp, value int, " +
                    "PRIMARY KEY ((id), time))");
            //Создаю таблицу для результата
            session.execute("CREATE TABLE IF NOT EXISTS hw2.result (id int, time timestamp, scale varchar, value int, " +
                    "PRIMARY KEY ((id), time, scale))");
        }
    }

    /**
     * Write metric to Cassandra
     * @param rdd JavaRDD of aggregated metrics
     */
    public void writeMetric(JavaRDD<MetricDto> rdd) {
        CassandraJavaUtil.
                javaFunctions(rdd)
                .saveToCassandra("hw2",
                        "metric",
                        CassandraJavaUtil.mapToRow(MetricDto.class),
                        CassandraJavaUtil.someColumns("id", "time", "value")
                );
    }

    /**
     * Read RDD with metrics from Cassandra
     * @return RDD with log messages' information
     */
    public JavaRDD<MetricDto> readMetrics() {
        return CassandraJavaUtil.javaFunctions(sparkContext)
                .cassandraTable("hw2", "metric", CassandraJavaUtil.mapRowTo(MetricDto.class))
                //Select only log message time and priority
                .select("id", "time", "value");
    }

    /**
     * Write aggregated metric to Cassandra
     * @param rdd JavaRDD of aggregated metrics
     */
    public void writeAggregatedMetric(JavaRDD<AggregatedMetricDto> rdd) {
        CassandraJavaUtil.
                javaFunctions(rdd)
                .saveToCassandra("hw2",
                        "result",
                        CassandraJavaUtil.mapToRow(AggregatedMetricDto.class),
                        CassandraJavaUtil.someColumns("id", "time", "scale", "value")
                );
    }
}
