package uk.richardconnor.metricSpaceFramework.coreConcepts;

public class CountedMetric<T> implements Metric<T> {

    int count;
    Metric<T> m;

    public CountedMetric(Metric<T> m) {
        this.count = 0;
        this.m = m;
    }

    @Override
    public double distance(T x, T y) {
        this.count++;
        return this.m.distance(x, y);
    }

    @Override
    public String getMetricName() {
        return m.getMetricName();
    }

    public int reset() {
        int res = this.count;
        this.count = 0;
        return res;
    }

}

