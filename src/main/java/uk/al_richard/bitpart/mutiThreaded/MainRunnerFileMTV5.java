package uk.al_richard.bitpart.mutiThreaded;

import uk.al_richard.bitpart.referenceImplementation.ExclusionZone;
import uk.al_richard.bitpart.referenceImplementation.RefPointSet;
import uk.al_richard.bitpart.util.ListofLists;
import uk.richardconnor.metricSpaceFramework.coreConcepts.CountedMetric;
import uk.richardconnor.metricSpaceFramework.coreConcepts.Metric;
import uk.richardconnor.metricSpaceFramework.datapoints.CartesianPoint;
import uk.richardconnor.metricSpaceFramework.testloads.TestContext;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static java.util.concurrent.Executors.newFixedThreadPool;
import static uk.al_richard.bitpart.util.BitUtils.bits_to_longs;

/*
 * Multi threaded V5.
 * Uses 2D array of longs arranged row wise (for cache coherence).
 * Phases 2 and 3 in different threads. Synchronisation via Futures.
 * BlockResult data structure collects phase 2 results.
 * Block result has ref into original data and block of results.
 * Phase 2 results processed by parallel threads in phase 3.
 * Idea is to avoid cache misses in phase 2.
 */

public class MainRunnerFileMTV5 extends MainRunnerFile {

    public static void main(String[] args) throws InterruptedException, ExecutionException {

        TestContext.Context context = TestContext.Context.euc20;

        int noOfRefPoints = 50;
        int nChoose2 = ((noOfRefPoints - 1) * noOfRefPoints) / 2;
        int noOfBitSets = nChoose2 + (noOfRefPoints * ball_radii.length);

        int degree_of_parallelism = 4; //Runtime.getRuntime().availableProcessors();
        ExecutorService global_executor = newFixedThreadPool(degree_of_parallelism);

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

        System.out.println("codebase\t" + "MainRunnerFileMTV5" );
        System.out.println("context\t" + context);
        System.out.println("Degree of parallelism\t" + degree_of_parallelism );
        System.out.println("data size\t" + data.size());
        System.out.println("query size\t" + querySize);
        System.out.println("threshold\t" + threshold);
        System.out.println("refs size\t" + refs.size());
        System.out.println("no of bitsets\t" + noOfBitSets);

        // buildAndQueryVpt(tc);

        RefPointSet<CartesianPoint> rps = new RefPointSet<>(refs, tc.metric());
        List<ExclusionZone<CartesianPoint>> ezs = new ArrayList<>();

        MainRunnerFile.addSheetExclusions(data, refs, rps, ezs, false, false); // No 4 point or balance impl
        int num_ses = ezs.size();
        System.out.println("sheet exclusions size:\t" + ezs.size());
        MainRunnerFile.addBallExclusions(data, refs, rps, ezs, false); // no balance impl
        System.out.println("ball exclusions size:\t" + (ezs.size() - num_ses));
        System.out.println("exclusion zones size:\t" + ezs.size());


        long[][] datarep = new long[ bits_to_longs( data.size() ) ][ noOfBitSets ];  // rows are all noOfBitSets (in bits) columns in size, each array row is the bits 0..64 of the rows, all initialised to 0L (by default)

        buildBitSetData(data, datarep, rps, ezs);

        System.out.println("Querying ");

        queryBitSetData(queries, data, tc.metric(), threshold, datarep, rps, ezs, noOfRefPoints, global_executor, degree_of_parallelism);
        global_executor.shutdown();

    }



    @SuppressWarnings("boxing")
    static <T> void queryBitSetData(List<T> queries, List<T> data,
                                    Metric<T> metric, double threshold, long[][] datarep,
                                    RefPointSet<T> rps, List<ExclusionZone<T>> ezs, int noOfRefPoints, final ExecutorService global_executor, int degree_of_parallelism) throws InterruptedException, ExecutionException {

        int noOfResults = 0;
        int partitionsExcluded = 0;
        List<Integer> distance_counts = new ArrayList<>();

        long t0 = System.currentTimeMillis();

        int count = 0;

        long find_exclusions_time = 0l;
        long do_exclusions_time = 0l;
        long filter_time = 0l;

        for (T q : queries) {
            List<T> res = new ArrayList<>();
            double[] dists = rps.extDists(q, res, threshold);

            List<Integer> mustBeIn = new ArrayList<>();
            List<Integer> cantBeIn = new ArrayList<>();

            for (int i = 0; i < ezs.size(); i++) {
                ExclusionZone<T> ez = ezs.get(i);
                if (ez.mustBeIn( dists, threshold)) {
                    mustBeIn.add(i);
                } else if (ez.mustBeOut( dists, threshold)) {
                    cantBeIn.add(i);
                }
            }

            partitionsExcluded += cantBeIn.size() + mustBeIn.size();

            long t3 = System.currentTimeMillis();

            List<T> query_results = doExclusionsMT(datarep, data.size(), mustBeIn, cantBeIn, global_executor, degree_of_parallelism, data, threshold, distance_counts, metric, q, res);

            long t4 = System.currentTimeMillis();

            do_exclusions_time += t4 - t3;

            noOfResults += query_results.size();
        }

        long total_time = (System.currentTimeMillis() - t0);

        int dists = 0;
        for( Integer i : distance_counts ) {
            dists += i;
        }

        System.out.println("bitsets");
        System.out.println("dists per query\t"
                + (dists / queries.size() + noOfRefPoints));
        System.out.println("results\t" + noOfResults);
        System.out.println("time\t" + total_time / (float) queries.size());
        System.out.println("find_exclusions_time\t" + find_exclusions_time / (float) queries.size()  );
        System.out.println("do_exclusions_time\t" + do_exclusions_time  / (float) queries.size() );
//        System.out.println("filter_time\t" + filter_time  / (float) queries.size() + " " + filter_time * 100 / total_time + "%" );
        System.out.println("partitions excluded\t"
                + ((double) partitionsExcluded / queries.size()));
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

    protected static <T> List<T> doExclusionsMT(final long[][] datarep, final int dataSize,
                                                List<Integer> mustBeIn, List<Integer> cantBeIn,
                                                final ExecutorService executor, final int degree_of_parallelism,
                                                List<T> dat, double t, List<Integer> distance_calcs, Metric<T> metric, T q, List<T> res) throws InterruptedException, ExecutionException {

        final int dataSize_in_longs = bits_to_longs( dataSize );

        int blocksize_in_longs = dataSize_in_longs / degree_of_parallelism;
        if( dataSize_in_longs % blocksize_in_longs !=0 ) {  // round up if non integral value.
            blocksize_in_longs += 1;
        }

        final int final_blocksize_in_longs = blocksize_in_longs;
        final int number_blocks = degree_of_parallelism; // keep this as a separate variable - may change this!

        List<Future<BlockResult>> block_result_futures = new ArrayList<>();

        for (int block = 0; block < number_blocks; block++) {  // spark off number_blocks parallel computations

            final int block_start_in_longs = block * final_blocksize_in_longs;

            block_result_futures.add(executor.submit(() -> {

                // Each thread contributes blocksize_in_longs assignments to the block_results bitset.
                // Last one may not due to rounding.
                // if block_uninitialised remains true at end of column tests (in for loop) we are conservative and set all bits in block to 1 (-true).

                final long[] block_results = new long[ final_blocksize_in_longs ]; // rounds up to ensure capacity - potentially slightly too big.

                for (int long_in_block = 0; long_in_block < block_results.length; long_in_block++) { // iterate over the rows (of longs) in the block

                    long result = 0l;
                    boolean block_uninitialised = true; // has this block been assigned yet? - one block_uninitialised per block and per thread - so thread safe

                    int source_row_index = block_start_in_longs + long_in_block;

                    if( source_row_index < datarep.length ) { // might be over due to long rounding

                        long[] long_row = datarep[source_row_index];

                        for (int column : mustBeIn) {  // iterate over the columns
                            if (block_uninitialised) {
                                result = long_row[column];
                                block_uninitialised = false;
                            } else {
                                result &= long_row[column];
                            }
                        }
                        for (int column : cantBeIn) {  // iterate over the columns
                            if (block_uninitialised) {
                                result = ~long_row[column];                             // and not
                                block_uninitialised = false;
                            } else {
                                result &= ~long_row[column];                            // and not
                            }
                        }
                        if (block_uninitialised) {  //  not excluded or included - don't know case - must be conservative and include.
                            result = -1l; // set all bits to 1
                        }
                        block_results[long_in_block] = result;
                    }
                }
                return new BlockResult( block_start_in_longs, block_results ) ;  //
                } ) );
        };

        List<Future<List<T>>> partial_results_list_futures = new ArrayList<>();

        for(Future<BlockResult> f: block_result_futures) {

            partial_results_list_futures.add(executor.submit(() -> {
                return filter(dat, t, q, distance_calcs, metric, f.get().block_start_in_longs, f.get().block_results );
            } ) );
        }

        ListofLists ll = new ListofLists<>();
        ll.add(res);
        for(Future<List<T>> f: partial_results_list_futures) {
                ll.add( f.get() );
        }
        return ll;
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
