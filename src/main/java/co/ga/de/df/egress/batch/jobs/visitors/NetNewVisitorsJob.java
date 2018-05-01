package co.ga.de.df.egress.batch.jobs.visitors;

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
public class NetNewVisitorsJob {
    private static final Logger logger = LoggerFactory.getLogger(NetNewVisitorsJob.class);
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
    public Tasklet netNewVisitorsRegistryUpdateTasklet(){
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataWarehouseBackend);
        return new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                jdbcTemplate.update("INSERT INTO registration.visitor\n" +
                        "SELECT visitor_id::varchar(36),\n" +
                        "       event_id::varchar(36),\n" +
                        "       event_fingerprint::varchar(36),\n" +
                        "       collector_tstamp as acquired_at\n" +
                        " FROM (\n" +
                        "   WITH visitor_id_sources as (\n" +
                        "     SELECT\n" +
                        "       root_id,\n" +
                        "       visitor_id\n" +
                        "     FROM atomic.co_ga_lms_path_complete_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_lms_lms_feedback_score_submit_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_lms_next_activity_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_lms_lesson_start_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_lms_show_answer_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_lms_show_hint_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_lms_experienced_activity_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_lms_complete_activity_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_lms_path_assign_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_lms_show_card_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_lms_answer_question_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_lms_lms_feedback_comment_submit_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_lms_tc_begin_lesson_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_lms_path_start_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_lms_review_lesson_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_lms_tc_resume_lesson_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_lms_begin_activity_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_lms_path_unassign_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_lms_attempt_answer_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_lms_user_context_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_lms_pause_lesson_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_lms_previous_activity_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_lms_lesson_resume_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_lms_lesson_complete_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_lms_tc_complete_lesson_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_invoice_account_claim_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_user_deactivated_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_resource_requested_2\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_cancelled_transferred_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_saml_migration_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_forgot_password_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_prospect_capture_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_payment_voided_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_refund_voided_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_resource_requested_3\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_invoice_approved_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_payment_settled_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_pre_enrolled_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_enrolled_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_sign_in_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_visitor_id_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_resource_requested_4\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_refund_settled_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_register_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_manual_refund_issued_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_register_2\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_sign_in_3\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_sign_out_2\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_sign_out_2\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_withdrawn_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_waitlisted_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_user_reactivated_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_refund_submitted_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_manual_payment_made_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_manual_payment_destroyed_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_sign_out_3\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_sign_out_3\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_register_3\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_sign_in_2\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_lead_capture_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_user_updated_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_payment_schedule_updated_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_sign_out_4\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_sign_out_4\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_prospect_capture_2\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_cancelled_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_withdrawn_transferred_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_manual_refund_destroyed_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_invoice_prepared_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_forgot_password_2\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_invoice_account_claim_2\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 visitor_id\n" +
                        "               FROM atomic.co_ga_payment_submitted_1\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 new_visitor_id as visitor_id\n" +
                        "               FROM atomic.co_ga_sign_out_2\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 new_visitor_id as visitor_id\n" +
                        "               FROM atomic.co_ga_sign_out_3\n" +
                        "     UNION ALL SELECT\n" +
                        "                 root_id,\n" +
                        "                 new_visitor_id as visitor_id\n" +
                        "               FROM atomic.co_ga_sign_out_4\n" +
                        "   )\n" +
                        "   SELECT\n" +
                        "     visitor_id_sources.visitor_id :: VARCHAR(36) as visitor_id,\n" +
                        "     events.event_id :: VARCHAR(36),\n" +
                        "     events.event_fingerprint :: VARCHAR(36),\n" +
                        "     events.collector_tstamp,\n" +
                        "     row_number()\n" +
                        "     OVER (\n" +
                        "       PARTITION BY visitor_id_sources.visitor_id\n" +
                        "       ORDER BY collector_tstamp )                as row_number\n" +
                        "   FROM atomic.events\n" +
                        "     JOIN visitor_id_sources on (root_id = event_id)\n" +
                        "     LEFT OUTER JOIN registration.visitor ON (visitor_id_sources.visitor_id :: VARCHAR(36) = visitor.visitor_id)\n" +
                        "   WHERE LENGTH(visitor_id_sources.visitor_id)=36" +
                        " ) as new_visitor_events\n" +
                        " WHERE row_number = 1");
                return RepeatStatus.FINISHED;
            }
        };
    }
    @Bean
    public Tasklet netNewVisitorsMeasureTasklet (){
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataWarehouseBackend);
        return new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                jdbcTemplate.query("SELECT DATE(acquired_at) as acquisition_date,\n" +
                        "      COUNT(distinct visitor_id) as net_new_visitors\n" +
                        "FROM registration.visitor\n" +
                        "JOIN atomic.events\n" +
                        "    USING( event_id )\n" +
                        "WHERE DATE(acquired_at)=CURRENT_DATE-1\n" +
                        "GROUP BY 1 ", new RowCallbackHandler() {
                    @Override
                    public void processRow(ResultSet rs) throws SQLException {
                        try {
                            anodot.send(
                                    MetricName.builder("net_new_visitors").build().asString(),
                                    Double.toString(rs.getDouble("net_new_visitors")),
                                    rs.getDate("acquisition_date").getTime(),
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
    protected Step netNewVisitorsRegistryUpdateStep() throws Exception {
        return this.steps.get("netNewVisitorsRegistryUpdateStep").tasklet(netNewVisitorsRegistryUpdateTasklet()).build();
    }
    @Bean
    protected Step netNewVisitorsMeasureStep() throws Exception {
        return this.steps.get("netNewVisitorsMeasureStep").tasklet(netNewVisitorsMeasureTasklet()).build();
    }

    @Bean
    public Job netNewVisitorsJob1() throws Exception {
        return this
                .jobs
                .get("NetNewVisitorsJob")
                .incrementer(new RunIdIncrementer())
                .start(netNewVisitorsRegistryUpdateStep())
                .next(netNewVisitorsMeasureStep())
                .build();
    }

}
