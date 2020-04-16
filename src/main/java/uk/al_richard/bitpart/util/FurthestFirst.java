package uk.al_richard.bitpart.util;

import coreConcepts.Metric;

import java.util.ArrayList;
import java.util.List;

/**
 * Furthest first greedy algorithm translated from Peter Christian's Python implementation
 */
public class FurthestFirst {

    public static Double mean_ref_distance = 0d;
    public static Double min_ref_distance = Double.MAX_VALUE;
    public static Double max_distance = Double.MIN_VALUE;

    /**
     *
     * @param data - the dataset from which to choose the furthest points
     * @param data_randomisation_seed - a random ref into the dataset to use as the first point in the search
     * @param metric - the metric being used to measure distance
     * @param required - the number of data points required
     * @return - required datapoints that are the furthest apart from each other
     */
    public static <T> List<T> furthestFirst(List<T> data, int data_randomisation_seed, Metric<T> metric, int required) {

        List<T> result = new ArrayList<>();

        mean_ref_distance = 0d;
        min_ref_distance = Double.MAX_VALUE;
        PointDistance<T> furthest_pair = maxDistance(data, metric);
        double max_distance = furthest_pair.distance;
        result.add( furthest_pair.point1 );
        result.add( furthest_pair.point2 );
//        System.out.println( "Start Max = " + max_distance);


        while (result.size() < required) {  // Do this loop until we have the required number of ref points.

            T candidate_ref_point = null;

            double loop_max_dist = Double.MIN_VALUE;

            for (T point : data) {              // for all the points in the data set

                if (!result.contains(point)) {      // that are not in the result set already

                    // Calculate minimum distance of the current data point to current list of data points

                    double this_min_dist = Double.MAX_VALUE;

                    for (T ref_point : result) {            // we are comparing to the set of points that we have already established as being furthest apart.

                        double dist = metric.distance(point, ref_point);
                        if (dist < this_min_dist) {
                            this_min_dist = dist;
                        }
                    }
                    // Check if this minimum distance is the largest distance in loop so far
                    if (this_min_dist > loop_max_dist) {
                        loop_max_dist = this_min_dist;
                        candidate_ref_point = point;
                    }
                }
            }
            // Add this new farthest data point to the selected ones

            result.add(candidate_ref_point);
//            System.out.println( "Next  point = " + loop_max_dist);

            mean_ref_distance += loop_max_dist;
            if (loop_max_dist < min_ref_distance) {
                min_ref_distance = loop_max_dist;
            }
            if (loop_max_dist > max_distance) {
                max_distance = loop_max_dist;
            }
        }

        mean_ref_distance = mean_ref_distance / required;

        return result;
    }

    /**
     *
     * @param data - the dataset from which to choose the furthest points
     * @param metric - the metric being used to measure distance
     * @return - required datapoints that are the furthest apart from each other
     */
    public static  <T>  PointDistance maxDistance(List<T> data, Metric<T> metric ) {

        double greatest_span = Double.MIN_VALUE;

        T point1 = data.get(0);

        T previous = null;
        T previous_previous = null;

        T furthest1 = null;
        T furthest2 = null;

        while (!point1.equals(previous_previous)) {

            for (int i = 0; i < data.size(); i++) {
                T point2 = data.get(i);
                if (!point1.equals(point2)) {
                    double dist = metric.distance(point1, point2);
                    if (dist > greatest_span) {
                        greatest_span = dist;
                        furthest1 = point1;
                        furthest2 = point2;
                    }
                }
            }
            previous_previous = previous;
            previous = point1;
            point1 = furthest2;
        }

        return new PointDistance( furthest1, furthest2, greatest_span );
    }

}
