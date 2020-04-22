package uk.al_richard.bitpart.util;

import uk.richardconnor.metricSpaceFramework.coreConcepts.Metric;
import uk.richardconnor.metricSpaceFramework.datapoints.CartesianPoint;
import uk.richardconnor.metricSpaceFramework.datapoints.cartesian.Euclidean;
import uk.richardconnor.metricSpaceFramework.testloads.CartesianThresholds;
import uk.richardconnor.metricSpaceFramework.testloads.TestLoad;

import java.util.List;

import static org.apache.commons.math3.special.Gamma.gamma;

public class BallVolumes {

    public static void main(String[] args) {

        try {

            int querySize = 100;
            int dataSize = 1000000;

            String metricName = "euc";

            int noOfRefPoints = 20;

            System.out.println( "dimensions" + "\t" + "metricName" + "\t" + "threshold" + "\t\t" + "radius"  + "ball volumeNDimBall" + "\t" + "enclosing radius" + "\t" + "enclosing volumeNDimBall"  + "\t"  + "proportion" + "\t"  + "ball/enclosing volumeNDimBall ");

            for( int dimensons = 5; dimensons <= 22; dimensons++ ) {
                test( dimensons, dataSize, querySize, metricName, noOfRefPoints );
            }

        } catch (Exception e) {
            System.out.println( "Fatal error in doExperiment" );
            e.printStackTrace();
        }

    }

    public static void test( int dimensions, int dataSize, int querySize, String metricName, int noOfRefPoints ) throws Exception {

        TestLoad tl = new TestLoad(dimensions, dataSize + querySize + noOfRefPoints, true, true );

        List<CartesianPoint> refs = tl.getQueries( noOfRefPoints );
        List<CartesianPoint> dat = tl.getDataCopy();

        Metric<CartesianPoint> metric = getMetric( metricName );

        double threshold = CartesianThresholds.getThreshold( metric.getMetricName(), dimensions, 1 ); // 1 is ppm

        Double max_distance = FurthestFirst.maxDistance(dat, metric ).distance;

        for( double proportion = 0.1; proportion <= 1.0; proportion += 0.1 ) {

            double radius = calculateBallRadius( proportion, max_distance, dimensions );
            double ball_volume = volumeNDimBall( radius,dimensions);

            double enclosing_radius = enclosingRadius(max_distance, dimensions);
            double enclosing_volume = volumeNDimBall( enclosing_radius,dimensions);

            System.out.println( dimensions + "\t" + metricName + "\t" + threshold + "\t" + radius + "\t"  + ball_volume + "\t" + enclosing_radius + "\t" + enclosing_volume + "\t" + proportion + "\t" + ball_volume / enclosing_volume );



        }
    }

    public static double volumeNDimBall(double r, int n) {

            return (Math.pow(Math.PI, n/2) / gamma( (n/2) + 1 )) * Math.pow(r, n);

    }

    /*
     * @return the radius of a ball enclosing points at max_distance apart using Jung's theorem to do calculation.
     */
    private static double enclosingRadius( double max_distance, int dimensions ) {
        return max_distance * Math.sqrt( ((double) dimensions) / ( 2 * ( dimensions + 1 )));
    }

    /**
     *
     * @param fraction - the volumetric fraction of the spanning ball that is required
     * @param max_distance - the maximum distance between points in the data set
     * @param dimensions - the number of dimensions over which we are operating
     * @return the ball radius that fills a fraction of the spanning ball for a ball with max span between nodes of max_distance
     */
    public static double calculateBallRadius(double fraction, double max_distance, int dimensions ) {
        double V = volumeNDimBall( enclosingRadius( max_distance, dimensions ), dimensions );

        return rootN( (( V * fraction * gamma( (dimensions/2) + 1  ) )/ Math.pow( Math.PI,dimensions/2)),  dimensions);
    }

    private static Metric<CartesianPoint> getMetric(String metricName) throws Exception {
        if( metricName.equals( "euc" ) ) {
            return new Euclidean<CartesianPoint>();
        } else {
            throw new Exception( "unrecognized metric name: " + metricName );
        }
    }

    /**
     * ]
     * @param num
     * @param n
     * @return the nth root of num
     */
    public static double rootN(double num, double n)
    {
        return Math.pow(Math.E, Math.log(num)/n);
    }


    public static long factorial(int number) {
        long result = 1;

        for (int factor = 2; factor <= number; factor++) {
            result *= factor;
        }

        return result;
    }


}
