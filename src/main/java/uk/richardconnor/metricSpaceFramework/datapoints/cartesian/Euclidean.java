package uk.richardconnor.metricSpaceFramework.datapoints.cartesian;

import uk.richardconnor.metricSpaceFramework.coreConcepts.Metric;
import uk.richardconnor.metricSpaceFramework.datapoints.CartesianPoint;


public class Euclidean<T extends CartesianPoint> implements Metric<T> {

    @Override
    public double distance(T x, T y) {
        double[] ys = y.getPoint();
        double acc = 0;
        int ptr = 0;
        for( double xVal : x.getPoint()){
            final double diff = xVal - ys[ptr++];
            acc += diff * diff;
        }
        return Math.sqrt(acc);
    }

    @Override
    public String getMetricName() {
        return "euc";
    }

}
