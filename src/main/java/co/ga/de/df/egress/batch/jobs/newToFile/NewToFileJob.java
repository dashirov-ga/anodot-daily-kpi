package co.ga.de.df.egress.batch.jobs.newToFile;

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
public class NewToFileJob {

    private static final Logger logger = LoggerFactory.getLogger(NewToFileJob.class);
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
    public Tasklet newToFileByCampaignCategoryTasklet(){
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataWarehouseBackend);
        return new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                jdbcTemplate.query("        -- Examine prospect capture in terms of marketing campaign\n" +
                        "        -- category. A new to file email address counts toward\n" +
                        "        -- cumulative -ALL- category and a specific category it\n" +
                        "        -- was assigned to. If NTF was not assigned a category,\n" +
                        "        -- it counts toward an -UNKNOWN- category.\n" +
                        "        SELECT\n" +
                        "        DATE(root_tstamp) AS cycle_date,\n" +
                        "        '-ALL-'           AS campaign_category,\n" +
                        "        count(1)          AS new_to_file\n" +
                        "        FROM atomic.co_ga_prospect_capture_2\n" +
                        "        WHERE email_address IS NOT NULL\n" +
                        "        AND co_ga_prospect_capture_2.first_time_submission IS TRUE\n" +
                        "        AND root_tstamp < CURRENT_DATE\n" +
                        "      AND DATE(root_tstamp) = CURRENT_DATE - 1\n" +
                        "              GROUP BY 1, 2\n" +
                        "              UNION ALL\n" +
                        "              SELECT\n" +
                        "              DATE(root_tstamp)                        AS cycle_date,\n" +
                        "              COALESCE(NULLIF(campaign_category,''), '-UNKNOWN-') AS campaign_category,\n" +
                        "              count(1)                                 AS new_to_file\n" +
                        "              FROM atomic.events\n" +
                        "              INNER JOIN atomic.co_ga_prospect_capture_2\n" +
                        "              ON (events.event_id = co_ga_prospect_capture_2.root_id)\n" +
                        "              WHERE email_address IS NOT NULL\n" +
                        "              AND co_ga_prospect_capture_2.first_time_submission IS TRUE\n" +
                        "              AND collector_tstamp < CURRENT_DATE\n" +
                        "      AND DATE(collector_tstamp) = CURRENT_DATE - 1\n" +
                        "              GROUP BY 1, 2;",
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
                                            MetricName.builder("new_to_file")
                                                    .withPropertyValue("campaign_category",rs.getString("campaign_category"))
                                                    .build()
                                                    .asString(),
                                            Double.toString(rs.getDouble("new_to_file")),
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
    public Tasklet newToFilebyTopicTasklet(){
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataWarehouseBackend);
        return new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                jdbcTemplate.query("-- Examine prospect capture in terms of topic of interest.\n" +
                                "-- Same NTF counts towards multiple topics of interest if\n" +
                                "-- multiple interests were expressed.\n" +
                                "-- New to File is defined as a prospect with email address\n" +
                                "-- defined, flagged as a first time submission.\n" +
                                "SELECT\n" +
                                "  DATE(collector_tstamp) AS cycle_date,\n" +
                                "  '-ALL-'                AS topic,\n" +
                                "  count(1)               AS new_to_file\n" +
                                "FROM atomic.events\n" +
                                "  INNER JOIN atomic.co_ga_prospect_capture_2\n" +
                                "    ON (events.event_id = co_ga_prospect_capture_2.root_id)\n" +
                                "  INNER JOIN atomic.co_ga_topic_context_1\n" +
                                "    ON (events.event_id = co_ga_topic_context_1.root_id)\n" +
                                "WHERE email_address IS NOT NULL\n" +
                                "      AND co_ga_prospect_capture_2.first_time_submission IS TRUE\n" +
                                "      AND collector_tstamp < CURRENT_DATE\n" +
                                "      AND DATE(collector_tstamp) = current_date - 1\n" +
                                "GROUP BY 1, 2\n" +
                                "UNION ALL\n" +
                                "SELECT\n" +
                                "  DATE(collector_tstamp) AS cycle_date,\n" +
                                "  COALESCE(NULLIF(slug,''),'-UNKNOWN-')AS topic,\n" +
                                "  count(1)               AS new_to_file\n" +
                                "FROM atomic.events\n" +
                                "  INNER JOIN atomic.co_ga_prospect_capture_2\n" +
                                "    ON (events.event_id = co_ga_prospect_capture_2.root_id)\n" +
                                "  INNER JOIN atomic.co_ga_topic_context_1\n" +
                                "    ON (events.event_id = co_ga_topic_context_1.root_id)\n" +
                                "WHERE email_address IS NOT NULL\n" +
                                "      AND co_ga_prospect_capture_2.first_time_submission IS TRUE\n" +
                                "      AND collector_tstamp < CURRENT_DATE\n" +
                                "      AND DATE(collector_tstamp) = current_date - 1\n" +
                                "GROUP BY 1, 2;\n",
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
                                            MetricName.builder("new_to_file")
                                                    .withPropertyValue("topic",rs.getString("topic"))
                                                    .build()
                                                    .asString(),
                                            Double.toString(rs.getDouble("new_to_file")),
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
    protected Step newToFileByCampaignCategoryStep() throws Exception {
        return this
                .steps.get("newToFileByCampaignCategoryStep")
                .tasklet(newToFileByCampaignCategoryTasklet())
                .build();
    }
    @Bean
    protected Step newToFilebyTopicStep() throws Exception {
        return this
                .steps.get("newToFilebyTopicStep")
                .tasklet(newToFilebyTopicTasklet())
                .build();
    }

    @Bean
    public Job newToFileJob1() throws Exception {
        return this
                .jobs.get("NewToFileJob")
                .incrementer(new RunIdIncrementer())
                .start(newToFileByCampaignCategoryStep())
                .next(newToFilebyTopicStep())
                .build();
    }
}

