package ru.mephi.chenko.consuming.dao;

import org.springframework.data.cassandra.core.cql.Ordering;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.core.mapping.Table;

import java.util.Date;

@Table
public class Metric {

    @PrimaryKeyColumn(
            ordinal = 0,
            type = PrimaryKeyType.PARTITIONED,
            ordering = Ordering.DESCENDING)
    private Long id;

    @PrimaryKeyColumn(
            ordinal = 1,
            type = PrimaryKeyType.CLUSTERED)
    private Date time;

    @Column
    private Integer value;

    /**
     * Metric non arguments constructor
     * @return Metric
     */
    public Metric() {
    }

    /**
     * Metric all arguments constructor
     * @param id Id of metric group
     * @param time Time of the metric
     * @param value Value of the metric
     * @return Metric
     */
    public Metric(Long id, Date time, Integer value) {
        this.id = id;
        this.time = time;
        this.value = value;
    }

    /**
     * Returns metric's group id
     * @return Metric's group id
     */
    public Long getId() {
        return id;
    }

    /**
     * Set metric's group id
     * @param id New id of metric group
     */
    public void setId(Long id) {
        this.id = id;
    }

    /**
     * Returns metric's time
     * @return Metric's time
     */
    public Date getTime() {
        return time;
    }

    /**
     * Set metric's time
     * @param time New metric's time
     */
    public void setTime(Date time) {
        this.time = time;
    }

    /**
     * Returns metric's value
     * @return Metric's value
     */
    public Integer getValue() {
        return value;
    }

    /**
     * Set metric's value
     * @param value New metric's value
     */
    public void setValue(Integer value) {
        this.value = value;
    }
}
