package ru.mephi.chenko.consuming.dao;

import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface MetricRepository extends CassandraRepository<Metric, Long> {
}
