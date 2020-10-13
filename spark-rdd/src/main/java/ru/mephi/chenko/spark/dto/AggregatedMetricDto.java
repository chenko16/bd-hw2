package ru.mephi.chenko.spark.dto;

import java.io.Serializable;
import java.util.Date;
import java.util.Objects;

public class AggregatedMetricDto implements Serializable {

    private Long id;

    private Date time;

    private String scale;

    private Integer value;

    /**
     * Aggregated metric non arguments constructor
     * @return Aggregated metric
     */
    public AggregatedMetricDto() {
    }

    /**
     * Aggregated metric all arguments constructor
     * @param id Id of metric group
     * @param time Time of the metric
     * @param scale Scale of aggregation
     * @param value Value of the metric
     * @return Aggregated metric
     */
    public AggregatedMetricDto(Long id, Date time, String scale, Integer value) {
        this.id = id;
        this.time = time;
        this.scale = scale;
        this.value = value;
    }

    /**
     * Returns aggregated metric's group id
     * @return Aggregated metric's group id
     */
    public Long getId() {
        return id;
    }

    /**
     * Set aggregated metric's group id
     * @param id New id of aggregated metric group
     */
    public void setId(Long id) {
        this.id = id;
    }

    /**
     * Returns aggregated metric's time
     * @return Aggregated metric's time
     */
    public Date getTime() {
        return time;
    }

    /**
     * Set aggregated metric's time
     * @param time New aggregated metric's time
     */
    public void setTime(Date time) {
        this.time = time;
    }

    /**
     * Returns scale metric has been aggregated by
     * @return Aggregated metric's scale
     */
    public String getScale() {
        return scale;
    }

    /**
     * Set scale metric has been aggregated by
     * @param scale New aggregated metric's scale
     */
    public void setScale(String scale) {
        this.scale = scale;
    }

    /**
     * Returns aggregated metric's value
     * @return Aggregated metric's value
     */
    public Integer getValue() {
        return value;
    }

    /**
     * Set aggregated metric's value
     * @param value New aggregated metric's value
     */
    public void setValue(Integer value) {
        this.value = value;
    }

    /**
     * Indicates whether some other object is "equal to" this one.
     * @param o Object compare with
     * @return Equals flag
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AggregatedMetricDto that = (AggregatedMetricDto) o;
        return id.equals(that.id) &&
                time.equals(that.time) &&
                scale.equals(that.scale) &&
                value.equals(that.value);
    }

    /**
     * Returns a hash code value for the object.
     * @return Hash code
     */
    @Override
    public int hashCode() {
        return Objects.hash(id, time, scale, value);
    }
}
