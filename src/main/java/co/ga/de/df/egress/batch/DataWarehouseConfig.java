package co.ga.de.df.egress.batch;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;


@Configuration
@PropertySource("classpath:warehouse-datasource.properties")
public class DataWarehouseConfig {
    private static final Logger logger = LoggerFactory.getLogger(DataWarehouseConfig.class);

    @Autowired
    Environment env;

    @ConfigurationProperties(prefix = "warehouse.datasource")
    @Bean("dataWarehouseBackend")
    DataSource dataSource(){
        logger.info("--->[ Loading data warehouse data source: {}]<---", env.getProperty("jdbcUrl"));
        return DataSourceBuilder.create().build();
    }

}
