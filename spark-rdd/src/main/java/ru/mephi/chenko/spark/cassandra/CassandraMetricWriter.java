package ru.mephi.chenko.spark.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import ru.mephi.chenko.spark.dto.AggregatedMetricDto;

public class CassandraMetricWriter {

    private final JavaSparkContext sparkContext;

    /**
     * CassandraMetricWriter all arguments constructor
     * @param sparkContext Context of job
     * @return KafkaMetricConsumer
     */
    public CassandraMetricWriter(JavaSparkContext sparkContext) {
        this.sparkContext = sparkContext;

        //Создаю коннектор к  Cassandra для native запроса
        CassandraConnector connector = CassandraConnector.apply(sparkContext.getConf());

        //Создаю таблицу для результата
        try (CqlSession session = connector.openSession()) {
            session.execute("CREATE TABLE IF NOT EXISTS hw2.metric (id int, time timestamp, scale varchar, value int, " +
                    "PRIMARY KEY ((id), time))");
        }
    }

    /**
     * Write metric to Cassandra
     * @param rdd JavaRDD of aggregated metrics
     */
    public void writeMetric(JavaRDD<AggregatedMetricDto> rdd) {
        CassandraJavaUtil.
                javaFunctions(rdd)
                .saveToCassandra("hw2",
                        "metric",
                        CassandraJavaUtil.mapToRow(AggregatedMetricDto.class),
                        CassandraJavaUtil.someColumns("id", "time", "scale", "value")
                );
    }
}
