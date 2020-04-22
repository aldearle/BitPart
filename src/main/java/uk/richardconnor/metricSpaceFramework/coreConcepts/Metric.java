package uk.richardconnor.metricSpaceFramework.coreConcepts;

/**
 * @author Richard Connor
 *
 *         an interface describing a distance metric, normally used with the
 *         intent of creating a MetricSpace object. Note that this should be a
 *         true metric otherwise some other classes will not function correctly
 *
 * @param <T>
 *            the type over which the metric operates
 */
public interface Metric<T> {

    /**
     * the distance function of the metric
     *
     * this should be a proper metric, ie a positive function with symmetry,
     * psuedo-identity and triangle inequality over the space of type T objects
     *
     * @param x
     *            the first parameter
     * @param y
     *            the second parameter
     * @return the distance between the parameters
     */
    public double distance(T x, T y);

    /**
     * it is important to give a useful name here so that the output of
     * experimental systems is correctly interpreted
     *
     * @return a name for a metric
     */
    public String getMetricName();

}

