package co.ga.de.df.egress.batch;

import com.anodot.metrics.spec.MetricName;

public class KPI {
    MetricName name;
    long time;
    double value;

    public MetricName getName() {
        return name;
    }

    public void setName(MetricName name) {
        this.name = name;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }
}
