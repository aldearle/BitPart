package uk.al_richard.bitpart.util;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DistanceHistogram {

    private static final int FIRST_FEW = 20;

    //private static final int FIRST_FEW = 30;         // number of points to miss out when calculating gradient
    private List<double[]> all_distances;

    private Double first_quartile_distance = Double.POSITIVE_INFINITY;
    private Double third_quartile_distance  = Double.POSITIVE_INFINITY;
    private double mean  = Double.POSITIVE_INFINITY;
    private double std_dev  = Double.POSITIVE_INFINITY;
    private double[] distances = null;
    private double min_distance;
    private double max_distance;


    public DistanceHistogram() {

        all_distances = new ArrayList<>();
    }

    /**
     * @return the distance 1/4 through the distribution or Double.POSITIVE_INFINITY if unset
     */
    public Double getFirstQuartileDistance() {
        return first_quartile_distance;
    }

    /**
     * @return the distance 3/4 through the distribution or Double.POSITIVE_INFINITY if unset
     */
    public Double getThirdQuartileDistance() {
        return third_quartile_distance;
    }

    /**
     * @return the mean distance or Double.POSITIVE_INFINITY if unset
     */
    public double getMean() {
        return mean;
    }

    /**
     * @return the std_dev or Double.POSITIVE_INFINITY if unset
     */
    public double getStdDev() {
        return std_dev;
    }

    public void add(double[] distances_from_datum_pivots ) {

        all_distances.add( distances_from_datum_pivots );
    }

    public double[] getDistances( int data_id ) {
        return all_distances.get( data_id );
    }

    /**
     * Sort the pivot-data distances
     * Then divide into BUCKET_COUNT buckets
     * The calculate the gradient of ln(distance) against count( points < d )
     * @return return the average averageIDIM for this collection
     */
    public void setMeasures() {

        int width = all_distances.get(0).length;
        int height = all_distances.size();

        distances = new double[width * height];
        int position = 0;
        for (double[] arrai : all_distances) {
            for (double d : arrai) {
                distances[position++] = d;
            }
        }
        Arrays.sort(distances);

        min_distance = distances[0];
        max_distance = distances[distances.length - 1];

        first_quartile_distance = distances[distances.length / 4];                // 1/4 of the measurements
        third_quartile_distance = distances[(distances.length * 3) / 4];          // 3/4 of the measurements

        mean = mean(distances);
        std_dev = stddev(distances, mean);
    }

    private Double logToBase(double value, double base) {
        return Math.log(value) / Math.log(base);
    }

    public double IDIM() {

        if( distances == null ) {
            throw new RuntimeException( "setMeasures must be called before calling IDIM" );
        }
        int BUCKET_COUNT = (distances.length < 200) ? distances.length : 200;

        double distance_increment = (max_distance - min_distance) / BUCKET_COUNT;
        double block_end = min_distance + distance_increment; // defines the end point of a set of measurements.

        ArrayList<Double> logLogPlotX = new ArrayList<>();
        ArrayList<Double> logLogPlotY = new ArrayList<>();

        for( int count = 1; count < distances.length; count++ ) {

            if( distances[ count ] > block_end  ) { // distance is past the end of the max distance for bucket so add counts to bucket

                logLogPlotX.add( Math.log(block_end) );
                logLogPlotY.add( Math.log(count) );

                block_end += distance_increment;
            }
        }

        return maxGradient(logLogPlotX, logLogPlotY);
    }

    private double maxGradient(ArrayList<Double> logLogPlotX, ArrayList<Double> logLogPlotY) {

        double max_gradient = Double.MIN_VALUE;

        Double [] X = logLogPlotX.toArray(new Double[logLogPlotX.size()]);
        Double [] Y = logLogPlotY.toArray(new Double[logLogPlotY.size()]);

        for( int i = 1; i < X.length; i++ ) {  // starts at 1 since no gradient possible for first point - cant look at X[i-1]
            double gradient = ( Y[i] - Y[i-1] ) / ( X[i] - X[i-1] );
            if( gradient > max_gradient && gradient != Double.POSITIVE_INFINITY && i > FIRST_FEW ) { // don't use infinite ones and miss out the first few since very steep at start.
                max_gradient = gradient;
            }
        }

        return max_gradient;
    }

    private double gradient(ArrayList<Double> logLogPlotX, ArrayList<Double> logLogPlotY) {

        double max_gradient = Double.MIN_VALUE;

        // We are plotting the log-log cdf graph of log(count of points at distance r) against log(r)

        Double [] X = logLogPlotX.toArray(new Double[logLogPlotX.size()]);
        Double [] Y = logLogPlotY.toArray(new Double[logLogPlotY.size()]);

        int no_points = X.length;

//        System.out.println( "-----" );
//        for( int i = 0; i < no_points; i++ ) {
//            System.out.println( X[i] + "\t" + Y[i] );
//        }
//        System.out.println( "-----" );

        // We expect the log log plot to have flat areas at end (for high X values)
        // Calculate the gradient ignoring the first and last specified % of measures.

        int ignore_percentage = 20;
        int percent_start = no_points / ignore_percentage;
        int percent_end = no_points - percent_start;

        double gradient = ( Y[percent_end] - Y[0] ) / ( X[percent_end] - X[0] );

        return gradient;

//        for( int i = 1; i < X.length; i++ ) {  // starts at 1 since no gradient possible for first point - cant look at X[i-1]
//            double gradient = ( Y[i] - Y[i-1] ) / ( X[i] - X[i-1] );
//
//            if( gradient != Double.POSITIVE_INFINITY && gradient > 0.00001 ) { // don't use infinite ones or zero
//                if( gradient > max_gradient ) {
//                    max_gradient = gradient;
//                }
//            }
//        }
//
//        return max_gradient;
    }

    public static double mean(double[] arrai) {
        double result = 0;
        for (double i : arrai) {
            result = result + i;
        }
        return result / arrai.length;
    }

    public static double stddev(double[] arrai, double mean) {
        double sd = 0;
        for (double i : arrai) {
            sd += Math.pow(i - mean, 2);
        }
        return Math.sqrt(sd / arrai.length);
    }


}
