package co.ga.de.df.egress.batch;

import org.junit.*;
import org.junit.runner.RunWith;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.test.context.junit4.SpringRunner;

import javax.sql.DataSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static org.junit.Assert.*;


@RunWith(SpringRunner.class)
@Import(DataWarehouseConfig.class)
@EnableAutoConfiguration
public class DataWarehouseConfigTest {

    @Autowired
    @Qualifier("dataWarehouseBackend")
    DataSource dataWarehouseBackend;

    public void setDataWarehouseBackend(DataSource dataWarehouseBackend) {
        this.dataWarehouseBackend = dataWarehouseBackend;
    }

    private JdbcTemplate jdbcTemplate;

    @Before
    public void setUp() throws Exception {
        jdbcTemplate = new JdbcTemplate(dataWarehouseBackend,false);
    }

    @Test
    public void dataSourceTest() {
        List<String> r = jdbcTemplate.query("SELECT USER;", (resultSet, i) -> resultSet.getString("current_user"));
        Assert.assertEquals(1,r.size());
        Assert.assertEquals("david_ashirov",r.get(0));
    }
}