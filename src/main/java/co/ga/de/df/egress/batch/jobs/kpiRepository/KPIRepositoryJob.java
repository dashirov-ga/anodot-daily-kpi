package co.ga.de.df.egress.batch.jobs.kpiRepository;

import com.anodot.metrics.Anodot;
import com.anodot.metrics.spec.MetricName;
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
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.List;

@Component
public class KPIRepositoryJob {
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
    protected Tasklet tasklet() {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataWarehouseBackend);
        return new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext context) throws IOException {
                List<KpiRepositoryDTO> kpis = jdbcTemplate.query(
                        "SELECT  * " +
                                "FROM atomic.kpi_repo " +
                                "WHERE cdate=CURRENT_DATE-1",
                        new KpiRepositoryRowMapper());
                for (KpiRepositoryDTO kpi: kpis) {
                    anodot.send(MetricName.builder(kpi.getKpi())
                            .withPropertyValue("team",kpi.getTeam())
                            .withPropertyValue("date_breakout",kpi.getDateBreakout())
                            .build().asString(),kpi.getCount().toPlainString(),kpi.getCycleDate().toEpochDay(),"counter");
                }
                anodot.flush();
                return RepeatStatus.FINISHED;
            }
        };
    }

    @Bean
    protected Step kpiRepositoryJobBridgeStep() throws Exception {
        return this.steps.get("KPIRepositoryJobBridgeStep").tasklet(tasklet()).build();
    }

    @Bean
    public Job kpiRepositoryJob() throws Exception {
        return this.jobs.get("KPIRepositoryJob")
                .incrementer(new RunIdIncrementer())
                .start(kpiRepositoryJobBridgeStep())
                .build();
    }



}
