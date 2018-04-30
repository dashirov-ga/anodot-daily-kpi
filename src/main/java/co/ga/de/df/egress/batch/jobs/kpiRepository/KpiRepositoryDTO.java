package co.ga.de.df.egress.batch.jobs.kpiRepository;

import java.math.BigDecimal;
import java.time.LocalDate;

public class KpiRepositoryDTO {
    private LocalDate cycleDate;
    private String dateBreakout;
    private String team;
    private String kpi;
    private BigDecimal count;

    public LocalDate getCycleDate() {
        return cycleDate;
    }

    public void setCycleDate(LocalDate cycleDate) {
        this.cycleDate = cycleDate;
    }

    public String getDateBreakout() {
        return dateBreakout;
    }

    public void setDateBreakout(String dateBreakout) {
        this.dateBreakout = dateBreakout;
    }

    public String getTeam() {
        return team;
    }

    public void setTeam(String team) {
        this.team = team;
    }

    public String getKpi() {
        return kpi;
    }

    public void setKpi(String kpi) {
        this.kpi = kpi;
    }

    public BigDecimal getCount() {
        return count;
    }

    public void setCount(BigDecimal count) {
        this.count = count;
    }

    public void setCount(Double count) {
        this.count = new BigDecimal(count).setScale(2,BigDecimal.ROUND_HALF_UP);
    }

}
