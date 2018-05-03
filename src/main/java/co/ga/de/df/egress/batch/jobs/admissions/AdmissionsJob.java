package co.ga.de.df.egress.batch.jobs.admissions;

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
public class AdmissionsJob {

    private static final Logger logger = LoggerFactory.getLogger(AdmissionsJob.class);
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
    public Tasklet admissionsCallsMadeRegisterNewLeadsTasklet(){
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataWarehouseBackend);
        return new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                jdbcTemplate.execute("CREATE TABLE IF NOT EXISTS registration.lead (\n" +
                        "    lead_id VARCHAR(128) PRIMARY KEY NOT NULL DISTKEY ENCODE ZSTD ,\n" +
                        "    acquired_at TIMESTAMP NOT NULL SORTKEY ENCODE ZSTD\n" +
                        ") DISTSTYLE KEY ;\n");

                jdbcTemplate.execute("INSERT INTO registration.lead\n" +
                        "WITH NET_NEW_LEADS AS (\n" +
                        "    SELECT\n" +
                        "      task.lead__c                                                         AS lead_id,\n" +
                        "      row_number()\n" +
                        "      OVER (\n" +
                        "        PARTITION BY task.lead__c\n" +
                        "        ORDER BY coalesce(task.call_completed_datetime__c, task.created_date_time__c)\n" +
                        "        )                                                                  AS row_number,\n" +
                        "      coalesce(task.call_completed_datetime__c, task.created_date_time__c) AS acquired_at\n" +
                        "    FROM sd_salesforce_campus_database.task\n" +
                        "      LEFT OUTER JOIN registration.lead\n" +
                        "        ON (task.lead__c = lead.lead_id)\n" +
                        "    WHERE task.lead__c IS NOT NULL\n" +
                        "          AND (\n" +
                        "            (task.call__c = TRUE   -- call task\n" +
                        "             AND\n" +
                        "             task.call_outcome__c IS NOT NULL -- completed\n" +
                        "            ) -- a completed task/activity\n" +
                        "          )\n" +
                        "          AND lead.lead_id IS NULL -- net new to registration table\n" +
                        ") SELECT lead_id, acquired_at from NET_NEW_LEADS where row_number = 1;\n" +
                        "\n");
                return RepeatStatus.FINISHED;
            }
        };
    }

    @Bean
    public Tasklet admissionsCallsMadeMeasureTasklet(){
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataWarehouseBackend);
        return new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                jdbcTemplate.query("select acquired_at::date,\n" +
                                "       COALESCE(all_leads.first_program__c,'-UNKNOWN-') as first_program,\n" +
                                "       COALESCE(all_leads.first_source__c,'-UNKNOWN-')  as first_source,\n" +
                                "       COALESCE(all_leads.type__c,'-UNKNOWN-')          as type,\n" +
                                "       count(1) as admissions_screening_calls_made\n" +
                                "  from\n" +
                                "   sd_salesforce_campus_database.lead as all_leads\n" +
                                "     join\n" +
                                "       registration.lead as validated_lead\n" +
                                "   on (all_leads.lead_id_18__c = validated_lead.lead_id)\n" +
                                "where acquired_at::date  = current_date-1\n" +
                                "group by 1,2,3,4\n" +
                                "\n" +
                                "UNION ALL\n" +
                                "\n" +
                                "select acquired_at::date,\n" +
                                "  '-ALL-' as first_program,\n" +
                                "  COALESCE(all_leads.first_source__c,'-UNKNOWN-')  as first_source,\n" +
                                "  COALESCE(all_leads.type__c,'-UNKNOWN-')          as type,\n" +
                                "  count(1) as admissions_screening_calls_made\n" +
                                "from\n" +
                                "  sd_salesforce_campus_database.lead as all_leads\n" +
                                "  join\n" +
                                "  registration.lead as validated_lead\n" +
                                "    on (all_leads.lead_id_18__c = validated_lead.lead_id)\n" +
                                "where acquired_at::date  = current_date-1\n" +
                                "group by 1,2,3,4\n" +
                                "\n" +
                                "UNION ALL\n" +
                                "\n" +
                                "select acquired_at::date,\n" +
                                "  COALESCE(all_leads.first_program__c,'-UNKNOWN-') as first_program,\n" +
                                "  '-ALL-'  as first_source,\n" +
                                "  COALESCE(all_leads.type__c,'-UNKNOWN-')          as type,\n" +
                                "  count(1) as admissions_screening_calls_made\n" +
                                "from\n" +
                                "  sd_salesforce_campus_database.lead as all_leads\n" +
                                "  join\n" +
                                "  registration.lead as validated_lead\n" +
                                "    on (all_leads.lead_id_18__c = validated_lead.lead_id)\n" +
                                "where acquired_at::date  = current_date-1\n" +
                                "group by 1,2,3,4\n" +
                                "UNION ALL\n" +
                                "select acquired_at::date,\n" +
                                "  COALESCE(all_leads.first_program__c,'-UNKNOWN-') as first_program,\n" +
                                "  COALESCE(all_leads.first_source__c,'-UNKNOWN-')  as first_source,\n" +
                                "  '-ALL-'          as type,\n" +
                                "  count(1) as admissions_screening_calls_made\n" +
                                "from\n" +
                                "  sd_salesforce_campus_database.lead as all_leads\n" +
                                "  join\n" +
                                "  registration.lead as validated_lead\n" +
                                "    on (all_leads.lead_id_18__c = validated_lead.lead_id)\n" +
                                "where acquired_at::date  = current_date-1\n" +
                                "group by 1,2,3,4\n" +
                                "\n" +
                                "UNION ALL\n" +
                                "\n" +
                                "select acquired_at::date,\n" +
                                "  COALESCE(all_leads.first_program__c,'-UNKNOWN-') as first_program,\n" +
                                "  '-ALL-'  as first_source,\n" +
                                "  '-ALL-'          as type,\n" +
                                "  count(1) as admissions_screening_calls_made\n" +
                                "from\n" +
                                "  sd_salesforce_campus_database.lead as all_leads\n" +
                                "  join\n" +
                                "  registration.lead as validated_lead\n" +
                                "    on (all_leads.lead_id_18__c = validated_lead.lead_id)\n" +
                                "where acquired_at::date  = current_date-1\n" +
                                "group by 1,2,3,4\n" +
                                "\n" +
                                "UNION ALL\n" +
                                "\n" +
                                "select acquired_at::date,\n" +
                                "  '-ALL-' as first_program,\n" +
                                "  COALESCE(all_leads.first_source__c,'-UNKNOWN-')  as first_source,\n" +
                                "  '-ALL-'          as type,\n" +
                                "  count(1) as admissions_screening_calls_made\n" +
                                "from\n" +
                                "  sd_salesforce_campus_database.lead as all_leads\n" +
                                "  join\n" +
                                "  registration.lead as validated_lead\n" +
                                "    on (all_leads.lead_id_18__c = validated_lead.lead_id)\n" +
                                "where acquired_at::date  = current_date-1\n" +
                                "group by 1,2,3,4\n" +
                                "\n" +
                                "UNION ALL\n" +
                                "\n" +
                                "select acquired_at::date,\n" +
                                "  COALESCE(all_leads.first_program__c,'-UNKNOWN-') as first_program,\n" +
                                "  COALESCE(all_leads.first_source__c,'-UNKNOWN-')  as first_source,\n" +
                                "  '-ALL-'         as type,\n" +
                                "  count(1) as admissions_screening_calls_made\n" +
                                "from\n" +
                                "  sd_salesforce_campus_database.lead as all_leads\n" +
                                "  join\n" +
                                "  registration.lead as validated_lead\n" +
                                "    on (all_leads.lead_id_18__c = validated_lead.lead_id)\n" +
                                "where acquired_at::date  = current_date-1\n" +
                                "group by 1,2,3,4\n" +
                                "\n" +
                                "UNION ALL\n" +
                                "\n" +
                                "select acquired_at::date,\n" +
                                "  '-ALL-'  as first_program,\n" +
                                "  '-ALL-'  as first_source,\n" +
                                "  '-ALL-'  as type,\n" +
                                "  count(1) as admissions_screening_calls_made\n" +
                                "from\n" +
                                "  sd_salesforce_campus_database.lead as all_leads\n" +
                                "  join\n" +
                                "  registration.lead as validated_lead\n" +
                                "    on (all_leads.lead_id_18__c = validated_lead.lead_id)\n" +
                                "where acquired_at::date  = current_date-1\n" +
                                "group by 1,2,3,4;",
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
                                            MetricName.builder("admissions_screening_calls_made")
                                                    .withPropertyValue("first_program",rs.getString("first_program"))
                                                    .withPropertyValue("first_source",rs.getString("first_source"))
                                                    .withPropertyValue("type",rs.getString("type"))
                                                    .build()
                                                    .asString(),
                                            Double.toString(rs.getDouble("admissions_screening_calls_made")),
                                            rs.getDate("acquired_at").getTime(),
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
    public Tasklet admissionsCallsConnectedMeasureTasklet(){
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataWarehouseBackend);
        return new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                jdbcTemplate.query("SELECT\n" +
                                "  DATE(coalesce(call_completed_datetime__c, created_date_time__c)) as cycle_date,\n" +
                                "  '-ALL-' days_since_lead_acquisition,\n" +
                                "  count(DISTINCT (lead__c)) AS number_of_admissions_calls_connected\n" +
                                "FROM sd_salesforce_campus_database.task\n" +
                                "WHERE lead__c IS NOT NULL\n" +
                                "      AND (\n" +
                                "        -- call task\n" +
                                "        call__c = TRUE\n" +
                                "        AND\n" +
                                "        call_outcome__c ~* 'Call Connected|Meaningful Conv|Not Interested'  -- connected\n" +
                                "      )\n" +
                                "      AND\n" +
                                "      DATE(coalesce(call_completed_datetime__c, created_date_time__c)) = current_date - 1  -- happened in a time window\n" +
                                "GROUP BY 1,2\n" +
                                "\n" +
                                "UNION ALL\n" +
                                "\n" +
                                "SELECT\n" +
                                "  DATE(coalesce(call_completed_datetime__c, created_date_time__c))  as cycle_date,\n" +
                                "  CASE DATEDIFF(days, DATE(acquired_at),DATE(coalesce(call_completed_datetime__c, created_date_time__c)))::INTEGER / 10::INTEGER\n" +
                                "    WHEN 0             THEN '< 10'\n" +
                                "    WHEN 1             THEN '10-30'\n" +
                                "    WHEN 2             THEN '10-30'\n" +
                                "    WHEN 3             THEN '30-60'\n" +
                                "    WHEN 4             THEN '30-60'\n" +
                                "    WHEN 5             THEN '30-60'\n" +
                                "    ELSE '60 +'\n" +
                                "    END as days_since_lead_acquisition,\n" +
                                "  count(DISTINCT (lead__c)) AS number_of_admissions_calls_connected\n" +
                                "FROM sd_salesforce_campus_database.task\n" +
                                "  INNER JOIN registration.lead on ( lead.lead_id=lead__c )\n" +
                                "WHERE lead__c IS NOT NULL\n" +
                                "      AND (\n" +
                                "        -- call task\n" +
                                "        call__c = TRUE\n" +
                                "        AND\n" +
                                "        call_outcome__c ~* 'Call Connected|Meaningful Conv|Not Interested'  -- connected\n" +
                                "      )\n" +
                                "      AND\n" +
                                "      DATE(coalesce(call_completed_datetime__c, created_date_time__c)) = current_date - 1  -- happened in a time window\n" +
                                "GROUP BY 1,2",
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
                                            MetricName.builder("admissions_calls_connected")
                                                    .withPropertyValue("days_since_lead_acquisition",rs.getString("days_since_lead_acquisition"))
                                                    .build()
                                                    .asString(),
                                            Double.toString(rs.getDouble("number_of_admissions_calls_connected")),
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
    protected Step admissionsCallsMadeRegisterNewLeadsStep() throws Exception {
        return this
                .steps.get("admissionsCallsMadeRegisterNewLeadsStep")
                .tasklet(admissionsCallsMadeRegisterNewLeadsTasklet())
                .build();
    }
    @Bean
    protected Step admissionsCallsMadeMeasureStep() throws Exception {
        return this
                .steps.get("admissionsCallsMadeMeasureStep")
                .tasklet(admissionsCallsMadeMeasureTasklet())
                .build();
    }
    @Bean
    protected Step admissionsCallsConnectedMeasureStep() throws Exception {
        return this
                .steps.get("admissionsCallsConnectedMeasureStep")
                .tasklet(admissionsCallsConnectedMeasureTasklet())
                .build();
    }
    @Bean
    public Job admissionsJob1() throws Exception {
        return this
                .jobs.get("AdmissionsJob")
                .incrementer(new RunIdIncrementer())
                .start(admissionsCallsMadeRegisterNewLeadsStep())
                .next(admissionsCallsMadeMeasureStep())
                .next(admissionsCallsConnectedMeasureStep())
                .build();
    }
}

