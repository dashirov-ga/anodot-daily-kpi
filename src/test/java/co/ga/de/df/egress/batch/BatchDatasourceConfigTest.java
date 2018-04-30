package co.ga.de.df.egress.batch;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import javax.sql.DataSource;
import java.util.List;


@RunWith(SpringRunner.class)
@Import(SpringBatchConfig.class)
@EnableAutoConfiguration
public class BatchDatasourceConfigTest {

    @Autowired
    @Qualifier("springBatchBackend")
    DataSource springBatchBackend;

    public void setDataWarehouseBackend(DataSource springBatchBackend) {
        this.springBatchBackend = springBatchBackend;
    }

    private JdbcTemplate jdbcTemplate;

    @Before
    public void setUp() throws Exception {
        jdbcTemplate = new JdbcTemplate(springBatchBackend,false);
    }

    @Test
    public void dataSourceTest() {
        List<String> r = jdbcTemplate.query("SELECT USER;", (resultSet, i) -> resultSet.getString("current_user"));
        Assert.assertEquals(1,r.size());
        Assert.assertEquals("dashirov",r.get(0));
    }
}