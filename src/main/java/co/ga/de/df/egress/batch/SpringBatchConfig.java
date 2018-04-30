package co.ga.de.df.egress.batch;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.PropertySource;

import javax.sql.DataSource;


@Configuration
@PropertySource("classpath:batch-datasource.properties")
public class SpringBatchConfig {
    private static final Logger logger = LoggerFactory.getLogger(SpringBatchConfig.class);
    @ConfigurationProperties(prefix = "spring.datasource")
    @Bean("springBatchBackend")
    @Primary
    DataSource dataSource(){
        logger.info("--->[ Loading spring batch data source ]<---");
        return DataSourceBuilder.create().build();
    }

}
