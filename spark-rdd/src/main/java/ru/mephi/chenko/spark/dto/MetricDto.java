package ru.mephi.chenko.spark.dto;

import java.io.Serializable;
import java.util.Date;

public class MetricDto implements Serializable {

    private Long id;

    private Date time;

    private Integer value;

    /**
     * Metric non arguments constructor
     * @return Metric
     */
    public MetricDto() {
    }

    /**
     * Metric all arguments constructor
     * @param id Id of metric group
     * @param time Time of the metric
     * @param value Value of the metric
     * @return Metric
     */
    public MetricDto(Long id, Date time, Integer value) {
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
