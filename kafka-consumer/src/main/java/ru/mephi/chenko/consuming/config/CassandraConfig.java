package ru.mephi.chenko.consuming.config;

import com.datastax.oss.driver.api.core.CqlSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.SessionFactory;
import org.springframework.data.cassandra.config.CassandraEntityClassScanner;
import org.springframework.data.cassandra.config.CqlSessionFactoryBean;
import org.springframework.data.cassandra.config.SchemaAction;
import org.springframework.data.cassandra.config.SessionFactoryFactoryBean;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.SimpleUserTypeResolver;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;

@Configuration
@EnableCassandraRepositories(basePackages = { "ru.mephi.chenko.consuming.dao" })
public class CassandraConfig {

    @Value("${cassandra.host}")
    private String cassandraHost;

    @Value("#{T(java.lang.Integer).parseInt('${cassandra.port}')}")
    private Integer cassandraPort;

    @Value("${cassandra.keyspace-name}")
    private String keyspaceName;

    private static final String LOCAL_DATACENTER = "datacenter1";

    /**
     * Returns cassandra session
     * @return cassandra session
     */
    @Bean
    public CqlSessionFactoryBean session() {
        CqlSessionFactoryBean session = new CqlSessionFactoryBean();
        session.setContactPoints(cassandraHost);
        session.setPort(cassandraPort);
        session.setKeyspaceName(keyspaceName);
        session.setLocalDatacenter(LOCAL_DATACENTER);

        return session;
    }

    /**
     * Returns cassandra session factory
     * @return cassandra session factory
     */
    @Bean
    public SessionFactoryFactoryBean sessionFactory(CqlSession session, CassandraConverter converter) {
        SessionFactoryFactoryBean sessionFactory = new SessionFactoryFactoryBean();
        sessionFactory.setSession(session);
        sessionFactory.setConverter(converter);
        sessionFactory.setSchemaAction(SchemaAction.CREATE_IF_NOT_EXISTS);

        return sessionFactory;
    }

    /**
     * Returns cassandra mapping context
     * @return cassandra mapping context
     */
    @Bean
    public CassandraMappingContext mappingContext(CqlSession session) throws ClassNotFoundException {
        CassandraMappingContext mappingContext = new CassandraMappingContext();
        mappingContext.setUserTypeResolver(new SimpleUserTypeResolver(session));
        mappingContext.setInitialEntitySet(CassandraEntityClassScanner.scan(("ru.mephi.chenko.consuming.dao")));

        return mappingContext;
    }

    /**
     * Returns cassandra converter
     * @return cassandra converter
     */
    @Bean
    public CassandraConverter converter(CassandraMappingContext mappingContext) {
        return new MappingCassandraConverter(mappingContext);
    }

    /**
     * Returns cassandra template
     * @return cassandra template
     */
    @Bean
    public CassandraOperations cassandraTemplate(SessionFactory sessionFactory, CassandraConverter converter) {
        return new CassandraTemplate(sessionFactory, converter);
    }
}
