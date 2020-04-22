package uk.al_richard.bitpart.mutiThreaded;

import uk.al_richard.bitpart.referenceImplementation.ExclusionZone;
import uk.al_richard.bitpart.referenceImplementation.RefPointSet;
import uk.richardconnor.metricSpaceFramework.coreConcepts.CountedMetric;
import uk.richardconnor.metricSpaceFramework.coreConcepts.Metric;
import uk.richardconnor.metricSpaceFramework.datapoints.CartesianPoint;
import uk.richardconnor.metricSpaceFramework.testloads.TestContext;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/*
 * Multi threaded V7.
 * Attempt to create objects with encapsulated threads that hold horizontal state.
 * One thread per instance of BlockSlice that holds all horizontal computation state.
 * Each thread runs a dispatcher that receives messages from main program thread,
 * Encapsulated thread performs state initialisation and query computation.
 */

public class MainRunnerFileMTV7 extends MainRunnerFile {

    public static void main(String[] args) throws InterruptedException, ExecutionException, Exception {

        TestContext.Context context = TestContext.Context.euc20;

        int noOfRefPoints = 50;
        int nChoose2 = ((noOfRefPoints - 1) * noOfRefPoints) / 2;
        int noOfBitSets = nChoose2 + (noOfRefPoints * ball_radii.length);

        int degree_of_parallelism = 2; //Runtime.getRuntime().availableProcessors();

        TestContext tc = null;
        try {
            tc = new TestContext(context);
        } catch (Exception e) {
            System.out.println("Error initialising TestContext");
            System.exit(-1);
        }
        int querySize = (context == TestContext.Context.colors || context == TestContext.Context.nasa) ? tc
                .dataSize() / 10 : 1000;
        tc.setSizes(querySize, noOfRefPoints);
        List<CartesianPoint> data = tc.getData();

        List<CartesianPoint> refPool = tc.getRefPoints();
        List<CartesianPoint> refs = refPool;
        double threshold = tc.getThresholds()[0];

        List<CartesianPoint> queries = tc.getQueries();

        System.out.println("codebase\t" + "MainRunnerFileMTV7" );
        System.out.println("context\t" + context);
        System.out.println("Degree of parallelism\t" + degree_of_parallelism );
        System.out.println("data size\t" + data.size());
        System.out.println("query size\t" + querySize);
        System.out.println("threshold\t" + threshold);
        System.out.println("refs size\t" + refs.size());
        System.out.println("no of bitsets\t" + noOfBitSets);

        RefPointSet<CartesianPoint> rps = new RefPointSet<>(refs, tc.metric());
        List<ExclusionZone<CartesianPoint>> ezs = new ArrayList<>();

        MainRunnerFile.addSheetExclusions(data, refs, rps, ezs, false, false); // No 4 point or balance impl
        int num_ses = ezs.size();
        System.out.println("sheet exclusions size:\t" + ezs.size());
        MainRunnerFile.addBallExclusions(data, refs, rps, ezs, false); // no balance impl
        System.out.println("ball exclusions size:\t" + (ezs.size() - num_ses));
        System.out.println("exclusion zones size:\t" + ezs.size());

        ArrayList<BlockSlice<CartesianPoint>> block_slices = new ArrayList<>();

        for( int block = 0; block < degree_of_parallelism; block ++ ) {

            BlockSlice<CartesianPoint> bs = new BlockSlice<>(data, rps, ezs, data.size(), degree_of_parallelism, block, noOfBitSets);
            bs.start();
            block_slices.add( bs );
        }

        List<Future<Object>> futures = new ArrayList<>();

        for( BlockSlice<CartesianPoint> slice : block_slices ) {
            futures.add( slice.buildSliceBitSetData() );
        }

        // wait for those builds to finish
        for(Future<Object> f: futures) {
            Object rubbish = f.get();
        }

        System.out.println("Querying ");

        queryBitSetData(queries, tc.metric(), threshold, ezs, rps, block_slices, noOfRefPoints );

        for( BlockSlice<CartesianPoint> slice : block_slices ) {
            slice.terminate();
            // Threads should all die - don't need to wait.
        }
    }


    @SuppressWarnings("boxing")
    static void queryBitSetData(List<CartesianPoint> queries,
                                Metric<CartesianPoint> metric, double threshold, List<ExclusionZone<CartesianPoint>> ezs, RefPointSet<CartesianPoint> rps,
                                List<BlockSlice<CartesianPoint>> block_slices, int noOfRefPoints ) throws InterruptedException, ExecutionException {

        int noOfResults = 0;
        int distance_calcs = 0;

        long t0 = System.currentTimeMillis();

        for (CartesianPoint q : queries) {

            List<CartesianPoint> res = new ArrayList<>();
            double[] dists = rps.extDists(q, res, threshold);
            List<Integer> mustBeIn = new ArrayList<>();
            List<Integer> cantBeIn = new ArrayList<>();

            for (int i = 0; i < ezs.size(); i++) {
                ExclusionZone<CartesianPoint> ez = ezs.get(i);
                if (ez.mustBeIn( dists, threshold)) {
                    mustBeIn.add(i);
                } else if (ez.mustBeOut( dists, threshold)) {
                    cantBeIn.add(i);
                }
            }

            List<Future<List<CartesianPoint>>> futures = new ArrayList<>();

            for( BlockSlice<CartesianPoint> slice : block_slices ) {
                try {
                    futures.add(slice.querySlice(threshold, metric, q, mustBeIn, cantBeIn)); // Don't use diagnostic in this version of code
                } catch( Exception e ) {
                    System.out.println( "Exception in querySlice: " + e.getMessage() );
                }
            }

            for(Future<List<CartesianPoint>> f: futures) {
                try {
                    res.addAll(f.get());
                } catch( InterruptedException e1 ) {
                    System.out.println( "InterruptedException get from querySlice: " + e1.getMessage() );
                } catch( ExecutionException e2 ) {
                    System.out.println( "ExecutionException get from querySlice: " + e2.getMessage() );
                }
            }
            noOfResults += res.size();
        }


        long total_time = (System.currentTimeMillis() - t0);

        System.out.println("bitsets");
        System.out.println("results\t" + noOfResults);
        System.out.println("time\t" + total_time / (float) queries.size());
    }

    public static <T> void buildBitSetData(List<T> data, long[][] datarep,
                                           RefPointSet<T> rps, List<ExclusionZone<CartesianPoint>> ezs) {

        for (int bit_row = 0; bit_row < data.size(); bit_row++) {
            T p = data.get(bit_row);
            double[] dists = rps.extDists(p);
            for (int ez_index = 0; ez_index < ezs.size(); ez_index++) {
                boolean isIn = ezs.get(ez_index).isIn(dists);
                if (isIn) {
                    setbit( datarep, bit_row, ez_index );
                }
            }
        }
    }


    private static final int MASK = 63;

    private static void setbit(long[][] datarep, int bit_row, int bit_column) {

        /*
         * Assumes this is setting bit row, column to 1.
         * If we need to set to zero as well need an target &= (ALL_ONES - bit_row %  Long.SIZE);
         */

        int long_row_index = bit_row / Long.SIZE;
        long target = datarep[long_row_index][bit_column];
        target |= 1l << ( bit_row %  Long.SIZE );
        datarep[long_row_index][bit_column] = target;
    }




    public static <T> List<T> filter( List<T> dat, double t, T q, List<Integer> distance_calcs, Metric<T> metric, int block_start_in_longs, long[] block_results ) {
        CountedMetric<T> cm = new CountedMetric<>(metric);
        List<T> list_results = new ArrayList<>();
        int source_bit_index_start = block_start_in_longs * Long.SIZE;
        for (int long_in_block = 0; long_in_block < block_results.length; long_in_block++) {
            long long_bits = block_results[long_in_block];
            for (int bit_in_long = 0; bit_in_long < Long.SIZE; bit_in_long++) {
                int bit_index = source_bit_index_start + ( long_in_block * Long.SIZE ) + bit_in_long;
                if (bit_index < dat.size() && ((long_bits & (1l << bit_in_long)) != 0) /* ith bit set */ && cm.distance(q, dat.get(bit_index)) <= t) {
                    list_results.add(dat.get(bit_index));
                }
            }
        }
        distance_calcs.add( cm.reset() );
        return list_results;
    }

//    for (int long_in_block = 0; long_in_block < block_extent; long_in_block++) {
//        filter(dat,t,cm,q,these_results,block_size + long_in_block, results[long_in_block] );
//    }

    public static <T> void OLD_filter( List<T> dat, double t, CountedMetric<T> cm, T q, List<T> res, int long_index, long the_long_bits ) {
        int bit_index_start = long_index * Long.SIZE;
        for( int i = 0; i < Long.SIZE; i++ ) {
            int bit_index = bit_index_start+i;
            if (bit_index_start+i < dat.size() && ((the_long_bits & (1l << i)) != 0) && cm.distance(q,dat.get(bit_index)) <= t) {
                res.add(dat.get(bit_index));
            }
        }
    }
}
