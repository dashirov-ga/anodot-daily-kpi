package co.ga.de.df.egress.batch.jobs.kpiRepository;

import org.springframework.jdbc.core.RowMapper;

import java.math.RoundingMode;
import java.sql.ResultSet;
import java.sql.SQLException;

public class KpiRepositoryRowMapper implements RowMapper<KpiRepositoryDTO> {
    /**
     * Implementations must implement this method to map each row of data
     * in the ResultSet. This method should not call {@code next()} on
     * the ResultSet; it is only supposed to map values of the current row.
     *
     * @param rs     the ResultSet to map (pre-initialized for the current row)
     * @param rowNum the number of the current row
     * @return the result object for the current row (may be {@code null})
     * @throws SQLException if a SQLException is encountered getting
     *                      column values (that is, there's no need to catch SQLException)
     */
    @Override
    public KpiRepositoryDTO mapRow(ResultSet rs, int rowNum) throws SQLException {
        KpiRepositoryDTO kpiRepositoryDTO = new KpiRepositoryDTO();
        kpiRepositoryDTO.setCount(rs.getBigDecimal("count").setScale(2,RoundingMode.HALF_UP));
        kpiRepositoryDTO.setCycleDate(rs.getDate("cdate").toLocalDate());
        kpiRepositoryDTO.setDateBreakout(rs.getString("date_breakout"));
        kpiRepositoryDTO.setTeam(rs.getString("team"));
        kpiRepositoryDTO.setKpi(rs.getString("kpi"));
        return kpiRepositoryDTO;
    }
}
