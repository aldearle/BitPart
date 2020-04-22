package uk.al_richard.bitpart.production;

import uk.richardconnor.metricSpaceFramework.coreConcepts.CountedMetric;
import uk.richardconnor.metricSpaceFramework.coreConcepts.DataDistance;
import uk.richardconnor.metricSpaceFramework.coreConcepts.Metric;
import uk.al_richard.bitpart.mutiThreaded.BlockSlice;
import uk.al_richard.bitpart.referenceImplementation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.function.BiFunction;

import static uk.al_richard.bitpart.util.BallVolumes.calculateBallRadius;


/*****************************
 * TODO - this code has no balanced or four point parameters or functionality at the moment
 * *****************************/

/**
 * A packaged up parallel version of BitBlaster
 * No support for balanced and four point parameters.
 * ball radii at 1E-3, 1E-2, 0.1, 0.5, 0.8 of the maximum data distance in the data set
 *
 * @author al
 * 17/12/18
 * @param <T> - the type of the dataset
 */
public class ParallelBitBlaster<T> {

    private final BiFunction<T, T, Double> distfn;
    private final List<T> dat;
    private final CountedMetric cm;
    private final RefPointSet<T> rps;
    private final List<ExclusionZone<T>> ezs;
    private final ArrayList<BlockSlice<T>> block_slices;
    private boolean terminated;

    private static final int DEFAULT_DIMENSIONS = 10;
    private static final int DEFAULT_NO_REFS = 20;
    private static final long SEED = 87432768941L;
    private static final int MASK = 63;
    private static double[] ball_radii;

    /**
     * @param distfn - the distance function used to caclulate distances
     * @param dat - the data set over which queries will be made
     * @param parallel - true if multi-threading to be used - parallelism controlled by Runtime.getRuntime().availableProcessors()
     * @throws Exception - if anything goes wrong.
     */
    public ParallelBitBlaster(BiFunction<T, T, Double> distfn, List<T> dat, boolean parallel) throws Exception {
        this(distfn, dat, DEFAULT_NO_REFS, parallel, DEFAULT_DIMENSIONS);
    }

    /**
     * @param distfn - the distance function used to caclulate distances
     * @param dat - the data set over which queries will be made
     * @param parallel - true if multi-threading to be used - parallelism controlled by Runtime.getRuntime().availableProcessors()
     * @param dimensions - the number of dimensions of the data set - use a different constructor if unknown.
     * @throws Exception - if anything goes wrong.
     */
    public ParallelBitBlaster(BiFunction<T, T, Double> distfn, List<T> dat, boolean parallel, int dimensions) throws Exception {
        this(distfn, dat, DEFAULT_NO_REFS, parallel, dimensions );
    }

    /**
     * @param distfn - the distance function used to caclulate distances
     * @param dat - the data set over which queries will be made
     * @param no_ref_points - the number of reference points to be used in the model - use a different constructor if unsure.
     * @param parallel - true if multi-threading to be used - parallelism controlled by Runtime.getRuntime().availableProcessors()
     * @param dimensions - the number of dimensions of the data set - use a different constructor if unknown.
     * @throws Exception - if anything goes wrong.
     */
    public ParallelBitBlaster(BiFunction<T, T, Double> distfn, List<T> dat, int no_ref_points, boolean parallel, int dimensions ) throws Exception {
        this(distfn, chooseRandomReferencePoints(dat, no_ref_points), dat, dimensions, parallel);
    }

    /**
     * @param distfn - the distance function used to caclulate distances
     * @param dat - the data set over which queries will be made
     * @param refs - the set of reference points to be used - should be points drawn from dat
     * @param parallel - true if multi-threading to be used - parallelism controlled by Runtime.getRuntime().availableProcessors()
     * @param dimensions - the number of dimensions of the data set - use a different constructor if unknown.
     * @throws Exception - if anything goes wrong.
     */
    public ParallelBitBlaster(BiFunction<T, T, Double> distfn, List<T> refs, List<T> dat, int dimensions, boolean parallel) throws Exception {

        if( refs.size() < 2 ) {
            throw new Exception( "Number of reference objects must be >= 2" );
        }

        this.distfn = distfn;
        this.dat = dat;
        this.terminated = false;

        Metric<T> m = new Metric<T>() {

            @Override
            public double distance(T o1, T o2) {
                return distfn.apply(o1, o2);
            }

            @Override
            public String getMetricName() {
                return "supplied";
            }
        };

        cm = new CountedMetric(m);

        rps = new RefPointSet<T>(refs, m);
        ezs = new ArrayList<>();

        double max_distance = maxDistance(dat, m );

        ball_radii = new double[]{
                calculateBallRadius(1E-3, max_distance, dimensions),
                calculateBallRadius(1E-2, max_distance, dimensions),
                calculateBallRadius(0.1, max_distance, dimensions),
                calculateBallRadius(0.5, max_distance, dimensions),
                calculateBallRadius(0.8, max_distance, dimensions)
        };

        int noOfRefPoints = refs.size();
        int nChoose2 = ((noOfRefPoints - 1) * noOfRefPoints) / 2;
        int noOfBitSets = nChoose2 + (noOfRefPoints * ball_radii.length);

        int degree_of_parallelism = 1;
        if (parallel) {
            degree_of_parallelism = Runtime.getRuntime().availableProcessors();
        }

        addSheetExclusions(dat, refs, rps, ezs, false, false); // No 4 point or balance impl
        addBallExclusions(dat, refs, rps, ezs, false); // no balance impl

        block_slices = new ArrayList<>();

        for (int block = 0; block < degree_of_parallelism; block++) {

            BlockSlice<T> bs = new BlockSlice<T>(dat, rps, ezs, dat.size(), degree_of_parallelism, block, noOfBitSets);
            bs.start();
            block_slices.add(bs);
        }

        List<Future<Object>> futures = new ArrayList<>();

        for (BlockSlice<T> slice : block_slices) {
            futures.add(slice.buildSliceBitSetData());
        }

        // wait for those builds to finish
        for (Future<Object> f : futures) {
            Object rubbish = f.get();
        }
    }

    /**
     * Release the resources associated with this instance of BitBlaster.
     */
    public synchronized void terminate() {
        for (BlockSlice<T> slice : block_slices) {
            slice.terminate();
        }
        terminated = true;
    }

    /**
     * Find the nodes within range r of query.
     * @param q         - some data for which to find the neighbours within distance r
     * @param threshold the distance from query over which to search
     * @return all those nodes within r of @param X.
     */
    public synchronized List<DataDistance<T>> rangeSearch(final T q, final double threshold) throws Exception {

        if (terminated) {
            throw new Exception("Terminated");
        }

        List<DataDistance<T>> res = new ArrayList<>();
        double[] dists = rps.extDists(q);
        List<Integer> mustBeIn = new ArrayList<>();
        List<Integer> cantBeIn = new ArrayList<>();

        for (int i = 0; i < ezs.size(); i++) {
            ExclusionZone<T> ez = ezs.get(i);
            if (ez.mustBeIn( dists, threshold )) {
                mustBeIn.add(i);
            } else if (ez.mustBeOut( dists, threshold)) {
                cantBeIn.add(i);
            }
        }

        List<Future<List<DataDistance<T>>>> futures = new ArrayList<>();

        for (BlockSlice<T> slice : block_slices) {
            futures.add(slice.querySlice(threshold, cm, q, mustBeIn, cantBeIn)); // Don't use diagnostic in this version of code
        }

        for (Future<List<DataDistance<T>>> f : futures) {
            res.addAll(f.get());
        }

        return res;
    }


    /**
     * @return the size of the data set
     */
    public int size() {
        return dat.size();
    }

    //--------------------------------- private methods ---------------------------------

    /**
     *
     * @param data - the dataset from which Reference objects are chosen
     * @param number_of_reference_points the number of points to choose
     * @param <X> the type of the data
     * @return a list of randomly chosen points from data
     */
    private static <X> List<X> chooseRandomReferencePoints(final List<X> data, int number_of_reference_points) {

        if (number_of_reference_points >= data.size()) {
            return data;
        }

        List<X> reference_points = new ArrayList<>();
        Random random = new Random(SEED);

        while (reference_points.size() < number_of_reference_points) {
            X item = data.get(random.nextInt(data.size()));
            if (!reference_points.contains(item)) {
                reference_points.add(item);
            }
        }

        return reference_points;
    }

    private void addBallExclusions(List<T> dat, List<T> refs,
                                   RefPointSet<T> rps, List<ExclusionZone<T>> ezs, boolean balanced) {
        for (int i = 0; i < refs.size(); i++) {
            List<BallExclusion<T>> balls = new ArrayList<>();
            for (double radius : ball_radii) {
                BallExclusion<T> be = new BallExclusion<>(rps, i, radius);
                balls.add(be);
            }
            ezs.addAll(balls);
        }
    }

    private void addSheetExclusions(List<T> dat,
                                    List<T> refs, RefPointSet<T> rps,
                                    List<ExclusionZone<T>> ezs, boolean balanced,
                                    boolean fourPoint) {
        for (int i = 0; i < refs.size() - 1; i++) {
            for (int j = i + 1; j < refs.size(); j++) {

                SheetExclusion<T> se;
                if( fourPoint ) {
                    se = new SheetExclusion4p<T>(rps, i, j);
                } else {
                    se = new SheetExclusion3p<T>(rps, i, j);
                }
                if (balanced) {
                    se.setWitnesses(dat.subList(0, 1000));
                }
                ezs.add(se);
            }
        }
    }

    /**
     * @param data - the dataset from which to find the furthest points
     * @param metric - the metric being used to measure distance
     * @return - required datapoints that are the furthest apart from each other
     */
    private static <X> double maxDistance(List<X> data, Metric<X> metric ) {

        double greatest_span = Double.MIN_VALUE;

        X point1 = data.get(0);

        X previous = null;
        X previous_previous = null;

        X furthest1 = null;
        X furthest2 = null;

        while (!point1.equals(previous_previous)) {

            for (int i = 0; i < data.size(); i++) {
                X point2 = data.get(i);
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

        return greatest_span;
    }

}
