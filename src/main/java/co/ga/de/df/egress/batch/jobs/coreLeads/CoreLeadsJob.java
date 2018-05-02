package co.ga.de.df.egress.batch.jobs.coreLeads;

import com.anodot.metrics.Anodot;
import com.anodot.metrics.spec.MetricName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

@Component
public class CoreLeadsJob {

    private static final Logger logger = LoggerFactory.getLogger(CoreLeadsJob.class);
    @Autowired
    @Qualifier("dataWarehouseBackend")
    DataSource dataWarehouseBackend;

    @Autowired
    private JobBuilderFactory jobs;

    @Autowired
    private StepBuilderFactory steps;

    @Autowired
    private Anodot anodot;
    @Bean
    public Tasklet coreLeadsCapturedTasklet(){
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataWarehouseBackend);
        return new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                jdbcTemplate.query("SELECT events.collector_tstamp::DATE as cycle_date,\n" +
                                "       '-ALL-' as market_size,\n" +
                                "       '-All-' as metro,\n" +
                                "       count(1) as  core_leads_captured\n" +
                                "FROM atomic.co_ga_lead_capture_1\n" +
                                "  JOIN atomic.events on (root_id=event_id)\n" +
                                "WHERE DATE(root_tstamp ) = current_date -1\n" +
                                "GROUP BY 1,2,3\n" +
                                "UNION ALL\n" +
                                "SELECT\n" +
                                "  events.collector_tstamp::DATE as cycle_date,\n" +
                                "  CASE COALESCE(metro,'-UNKNOWN-')\n" +
                                "  WHEN 'Atlanta'           THEN 'Emerging'\n" +
                                "  WHEN 'Austin'            THEN 'Emerging'\n" +
                                "  WHEN 'Berlin'            THEN 'Emerging'\n" +
                                "  WHEN 'Boston'            THEN 'Emerging'\n" +
                                "  WHEN 'Brisbane'          THEN 'Emerging'\n" +
                                "  WHEN 'Chicago'           THEN 'Emerging'\n" +
                                "  WHEN 'Dallas'            THEN 'Emerging'\n" +
                                "  WHEN 'Denver'            THEN 'Emerging'\n" +
                                "  WHEN 'Hong Kong'         THEN 'Emerging'\n" +
                                "  WHEN 'Kansas City'       THEN 'Emerging'\n" +
                                "  WHEN 'London'            THEN 'Emerging'\n" +
                                "  WHEN 'Los Angeles'       THEN 'Emerging'\n" +
                                "  WHEN 'Melbourne'         THEN 'Emerging'\n" +
                                "  WHEN 'New York City'     THEN 'Established'\n" +
                                "  WHEN 'Online'            THEN 'Emerging'\n" +
                                "  WHEN 'Providence'        THEN 'Emerging'\n" +
                                "  WHEN 'San Diego'         THEN 'Emerging'\n" +
                                "  WHEN 'San Francisco'     THEN 'Established'\n" +
                                "  WHEN 'Seattle'           THEN 'Emerging'\n" +
                                "  WHEN 'Singapore'         THEN 'Emerging'\n" +
                                "  WHEN 'Sydney'            THEN 'Emerging'\n" +
                                "  WHEN 'Toronto'           THEN 'Emerging'\n" +
                                "  WHEN 'Washington, D.C.'  THEN 'Established'\n" +
                                "  WHEN 'Philadelphia'      THEN 'Emerging'\n" +
                                "  ELSE '-UNKNOWN-'\n" +
                                "  END as market_size,\n" +
                                "  '-ALL-' as metro,\n" +
                                "  count(1) as  core_leads_captured\n" +
                                "FROM atomic.co_ga_lead_capture_1\n" +
                                "  JOIN atomic.events on (root_id=event_id)\n" +
                                "WHERE DATE(root_tstamp ) = current_date -1\n" +
                                "GROUP BY 1,2,3\n" +
                                "UNION ALL\n" +
                                "SELECT\n" +
                                "  events.collector_tstamp::DATE as cycle_date,\n" +
                                "  CASE COALESCE(metro,'-UNKNOWN-')\n" +
                                "  WHEN 'Atlanta'           THEN 'Emerging'\n" +
                                "  WHEN 'Austin'            THEN 'Emerging'\n" +
                                "  WHEN 'Berlin'            THEN 'Emerging'\n" +
                                "  WHEN 'Boston'            THEN 'Emerging'\n" +
                                "  WHEN 'Brisbane'          THEN 'Emerging'\n" +
                                "  WHEN 'Chicago'           THEN 'Emerging'\n" +
                                "  WHEN 'Dallas'            THEN 'Emerging'\n" +
                                "  WHEN 'Denver'            THEN 'Emerging'\n" +
                                "  WHEN 'Hong Kong'         THEN 'Emerging'\n" +
                                "  WHEN 'Kansas City'       THEN 'Emerging'\n" +
                                "  WHEN 'London'            THEN 'Emerging'\n" +
                                "  WHEN 'Los Angeles'       THEN 'Emerging'\n" +
                                "  WHEN 'Melbourne'         THEN 'Emerging'\n" +
                                "  WHEN 'New York City'     THEN 'Established'\n" +
                                "  WHEN 'Online'            THEN 'Emerging'\n" +
                                "  WHEN 'Providence'        THEN 'Emerging'\n" +
                                "  WHEN 'San Diego'         THEN 'Emerging'\n" +
                                "  WHEN 'San Francisco'     THEN 'Established'\n" +
                                "  WHEN 'Seattle'           THEN 'Emerging'\n" +
                                "  WHEN 'Singapore'         THEN 'Emerging'\n" +
                                "  WHEN 'Sydney'            THEN 'Emerging'\n" +
                                "  WHEN 'Toronto'           THEN 'Emerging'\n" +
                                "  WHEN 'Washington, D.C.'  THEN 'Established'\n" +
                                "  WHEN 'Philadelphia'      THEN 'Emerging'\n" +
                                "  ELSE '-UNKNOWN-'\n" +
                                "  END as market_size,\n" +
                                "  COALESCE(metro,'-UNKNOWN-') as metro,\n" +
                                "  count(1) as  core_leads_captured\n" +
                                "FROM atomic.co_ga_lead_capture_1\n" +
                                "  JOIN atomic.events on (root_id=event_id)\n" +
                                "WHERE DATE(root_tstamp ) = current_date -1\n" +
                                "GROUP BY 1,2,3",
                        new RowCallbackHandler() {
                            /**
                             * Implementations must implement this method to process each row of data
                             * in the ResultSet. This method should not call {@code next()} on
                             * the ResultSet; it is only supposed to extract values of the current row.
                             * <p>Exactly what the implementation chooses to do is up to it:
                             * A trivial implementation might simply count rows, while another
                             * implementation might build an XML document.
                             *
                             * @param rs the ResultSet to process (pre-initialized for the current row)
                             * @throws SQLException if a SQLException is encountered getting
                             *                      column values (that is, there's no need to catch SQLException)
                             */
                            @Override
                            public void processRow(ResultSet rs) throws SQLException {
                                try {
                                    anodot.send(
                                            MetricName.builder("core_leads_captured")
                                                    .withPropertyValue("market_size",rs.getString("market_size"))
                                                    .withPropertyValue("metro",rs.getString("metro"))
                                                    .build()
                                                    .asString(),
                                            Double.toString(rs.getDouble("core_leads_captured")),
                                            rs.getDate("cycle_date").getTime(),
                                            "counter");
                                } catch (IOException e){
                                    e.printStackTrace();
                                }
                            }
                        });
                anodot.flush();
                return RepeatStatus.FINISHED;
            }
        };
    }

    @Bean
    protected Step coreLeadsCapturedStep() throws Exception {
        return this
                .steps.get("coreLeadsCapturedStep")
                .tasklet(coreLeadsCapturedTasklet())
                .build();
    }


    @Bean
    public Job coreLeadsCapturedJob1() throws Exception {
        return this
                .jobs.get("CoreLeadsCapturedJob")
                .incrementer(new RunIdIncrementer())
                .start(coreLeadsCapturedStep())
                .build();
    }
}

