package ru.mephi.chenko.dto;

import java.util.Date;

public class MetricDto {

    private Long id;

    private Date time;

    private Integer value;

    public MetricDto() {}

    public MetricDto(Long id, Date time, Integer value) {
        this.id = id;
        this.time = time;
        this.value = value;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Date getTime() {
        return time;
    }

    public void setTime(Date time) {
        this.time = time;
    }

    public Integer getValue() {
        return value;
    }

    public void setValue(Integer value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "TestDataDto [id=" + id + ", time=" + time + ", value=" + value + "]";
    }
}
