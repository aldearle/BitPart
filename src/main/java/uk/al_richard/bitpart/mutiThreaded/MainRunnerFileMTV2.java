package uk.al_richard.bitpart.mutiThreaded;

import uk.al_richard.bitpart.referenceImplementation.ExclusionZone;
import uk.al_richard.bitpart.referenceImplementation.RefPointSet;
import uk.al_richard.bitpart.util.FastPerms;
import uk.al_richard.bitpart.util.ListofLists;
import uk.al_richard.bitpart.util.OpenBitSet;
import uk.richardconnor.metricSpaceFramework.coreConcepts.CountedMetric;
import uk.richardconnor.metricSpaceFramework.coreConcepts.Metric;
import uk.richardconnor.metricSpaceFramework.datapoints.CartesianPoint;
import uk.richardconnor.metricSpaceFramework.testloads.TestContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static java.util.concurrent.Executors.newFixedThreadPool;
import static uk.al_richard.bitpart.util.BitUtils.bits_to_longs;


/**
 * Multi threaded V2.
 * Uses OpenBitSet for data stored column wise.
 * Phase 1 single threaded (very small execution time).
 * Phases 2 and 3 folded together.
 * Synchronisation using Futures.
 * Results collected in list of lists (from filtering).
 * Phase 2 results collected in sub blocks.
 *
 * @author al dearle (al@st-andrews.ac.uk)
 *
 */
public class MainRunnerFileMTV2 extends MainRunnerFile {

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

        System.out.println("codebase\t" + "MainRunnerFileMTV2" );
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

        OpenBitSet[] datarep = new OpenBitSet[noOfBitSets];
        MainRunnerFile.buildBitSetData(data, datarep, rps, ezs);

        System.out.println("Querying ");

        queryBitSetData(queries, data, tc.metric(), threshold, datarep, rps, ezs, noOfRefPoints, noOfBitSets, nChoose2, global_executor, degree_of_parallelism);
        global_executor.shutdown();

    }



    @SuppressWarnings("boxing")
    static <T> void queryBitSetData(List<T> queries, List<T> data,
                                    Metric<T> metric, double threshold, OpenBitSet[] datarep,
                                    RefPointSet<T> rps, List<ExclusionZone<T>> ezs, int noOfRefPoints, int noOfBitSets, int numberOfSheets, final ExecutorService global_executor, int degree_of_parallelism) throws InterruptedException, ExecutionException {

        int noOfResults = 0;
        int partitionsExcluded = 0;
        CountedMetric<T> cm = new CountedMetric<>(metric);
        long t0 = System.currentTimeMillis();

        int count = 0;

        long find_exclusions_time = 0l;
        long do_exclusions_time = 0l;
        long filter_time = 0l;

        for (T q : queries) {
            List<T> res = new ArrayList<>();
            double[] dists = rps.extDists(q, res, threshold);

            boolean[] mustBeIn = new boolean[noOfBitSets];
            boolean[] cantBeIn = new boolean[noOfBitSets];

            long t1 = System.currentTimeMillis();
            findExclusions(dists, threshold, mustBeIn, cantBeIn, ezs, numberOfSheets);
            long t2 = System.currentTimeMillis();

            int cantbein_size = countTrue( cantBeIn );
            int mustbein_size = countTrue( mustBeIn );

            partitionsExcluded += cantbein_size + mustbein_size;

            long t3 = System.currentTimeMillis();

            List<T> query_results = doExclusionsMT(datarep, data.size(), ezs.size(), mustBeIn, mustbein_size, cantBeIn, cantbein_size, global_executor, degree_of_parallelism, data, threshold, cm, q, res);

            long t4 = System.currentTimeMillis();

            find_exclusions_time += t2 - t1;
            do_exclusions_time += t4 - t3;

            noOfResults += query_results.size();
        }

        long total_time = (System.currentTimeMillis() - t0);

        System.out.println("bitsets");
        System.out.println("dists per query\t"
                + (cm.reset() / queries.size() + noOfRefPoints));
        System.out.println("results\t" + noOfResults);
        System.out.println("time\t" + total_time / (float) queries.size());
        System.out.println("find_exclusions_time\t" + find_exclusions_time / (float) queries.size()  );
        System.out.println("do_exclusions_time\t" + do_exclusions_time  / (float) queries.size() );
//        System.out.println("filter_time\t" + filter_time  / (float) queries.size() + " " + filter_time * 100 / total_time + "%" );
        System.out.println("partitions excluded\t"
                + ((double) partitionsExcluded / queries.size()));
    }

    private static void mustBeInOrOut(int index, double threshold, int number_sheets, double[] ball_radii, double[] dists, boolean[] mustBeIn, boolean[] cantBeIn) {

        if( index < number_sheets ) { // sheet exclusion
            int[] coords = FastPerms.getNthPair(index);
            int i = coords[0];
            int j = coords[1];
            if (dists[i] - (dists[j]) < -(2 * threshold)) {  // was sheet mustBeIn - only 3 point - return  (dists[this.ref1] - dists[this.ref2]) - this.offset < -(2 * t);
                mustBeIn[index] = true;
            } else if (dists[i] - (dists[j]) >= 2 * threshold) { // was sheet mustBeOut - only 3 point - return (dists[this.ref1] - dists[this.ref2]) - this.offset >= (2 * t);
                cantBeIn[index] = true;
            }
        } else { // ball exclusion

            int ro_index = (index - number_sheets) / ball_radii.length;         // which ro we are looking at
            int radius_index = (index - number_sheets) % ball_radii.length;     // ref of the radius relative to the current ro.

            double radius = ball_radii[radius_index];

            if (dists[ro_index] < radius - threshold) {   // was ball mustBeIn - return dists[this.ref] < this.radius - t;
                mustBeIn[index] = true;
            } else if (dists[ro_index] >= radius + threshold) { // was ball mustBeOut - return dists[this.ref] >= this.radius + t;
                cantBeIn[index] = true;
            }
        }
    }


    private static <T> void findExclusions(double[] dists, double threshold, final boolean[] mustBeIn, final boolean[] cantBeIn, List<ExclusionZone<T>> ezs, int numberOfSheets ) {

        for (int i = 0; i < ezs.size(); i++) {

            mustBeInOrOut(i, threshold, numberOfSheets, MainRunnerFile.ball_radii, dists, mustBeIn, cantBeIn);

        }
    }

    protected static <T> List<T> doExclusionsMT(final OpenBitSet[] datarep, final int dataSize, final int ezsSize,
                                                                 final boolean[] mustBeIn, final int mustbein_size, final boolean[] cantBeIn, final int cantbein_size,
                                                                 final ExecutorService executor, final int degree_of_parallelism,
                                                                 List<T> dat, double t, CountedMetric<T> cm, T q, List<T> res) throws InterruptedException, ExecutionException {

        final int dataSize_in_longs = bits_to_longs( dataSize );

        int blocksize_in_longs = dataSize_in_longs / degree_of_parallelism;
        if( dataSize_in_longs % blocksize_in_longs !=0 ) {  // round up if non integral value.
            blocksize_in_longs += 1;
        }

        final int final_blocksize_in_longs = blocksize_in_longs;
        final int number_blocks = degree_of_parallelism; // keep this as a separate variable - may change this!

        //final CountDownLatch latch = new CountDownLatch(degree_of_parallelism);
        List<Future<List<T>>> futures = new ArrayList<>();

        for (int block = 0; block < number_blocks; block++) {  // spark off number_blocks parallel computations

            final int block_size = block * blocksize_in_longs;
            final int block_extent = Math.min( final_blocksize_in_longs,dataSize_in_longs - block_size);

            //executor.execute(() -> {
            futures.add(executor.submit(() -> {

                // Each thread contributes blocksize_in_longs assignments to the results bitset.
                // Last one may not due to rounding.
                // if block_uninitialised remains true at end of column tests (in for loop) we are conservative and set all bits in block to 1 (-true).

                boolean block_uninitialised = true; // has this block been assigned yet? - one block_uninitialised per block and per thread - so thread safe
                final long[] results = new long[ final_blocksize_in_longs ]; // rounds up to ensure capacity - potentially slightly too big.

                for (int column_id = 0; column_id < ezsSize; column_id++) { // Test inclusion and exclusion for all columns

                    if (mustBeIn[column_id]) {
                        if (block_uninitialised) {
                            for (int long_in_block = 0; long_in_block < block_extent ; long_in_block++) {
                                results[long_in_block] = datarep[column_id].getLong(block_size + long_in_block);
                            }
                            block_uninitialised = false;
                        } else {
                            for (int long_in_block = 0; long_in_block < Math.min( final_blocksize_in_longs,dataSize_in_longs - block_size); long_in_block++) {
                                results[long_in_block] &= datarep[column_id].getLong(block_size + long_in_block);
                            }
                        }
                    }
                    if (cantBeIn[column_id]) {
                        if (block_uninitialised) {
                            for (int long_in_block = 0; long_in_block < block_extent; long_in_block++) {
                                results[long_in_block] = ~datarep[column_id].getLong(block_size + long_in_block);

                            }
                            block_uninitialised = false;
                        } else {
                            // and not:
                            for (int long_in_block = 0; long_in_block < block_extent; long_in_block++) {
                                results[long_in_block] &= ~datarep[column_id].getLong(block_size + long_in_block);
                            }
                        }
                    }
                    if (block_uninitialised) {  //  not excluded or included - don't know case - must be conservative and include.
                        for (int long_in_block = 0; long_in_block < final_blocksize_in_longs; long_in_block++) {
                                results[long_in_block] = -1l; // set all bits to 1
                        }
                    }
                }
                List<T> these_results = new ArrayList<>();
                for (int long_in_block = 0; long_in_block < block_extent; long_in_block++) {
                    filter(dat,t,cm,q,these_results,block_size + long_in_block, results[long_in_block] );
                }

                //latch.countDown();
                return these_results;
            }));
        };
        //latch.await();
        ListofLists ll = new ListofLists<>();
        ll.add(res);
        for(Future<List<T>> f: futures) {
            ll.add( f.get() );     // wait for all tasks to complete and add results to the res list
        }
        return ll;
        //executor.shutdown();
        //do_executor.awaitTermination(50l, TimeUnit.SECONDS);
    }

    public static <T> List<T> concatenatedList(List<T>... collections) {
        return Arrays.stream(collections).flatMap(Collection::stream).collect(Collectors.toList());
    }

    public static <T> void filter( List<T> dat, double t, CountedMetric<T> cm, T q, List<T> res, int long_index, long the_long_bits ) {
        int bit_index_start = long_index * Long.SIZE;
        for( int i = 0; i < Long.SIZE; i++ ) {
            int bit_index = bit_index_start+i;
            if (bit_index_start+i < dat.size() && ((the_long_bits & (1l << i)) != 0) && cm.distance(q,dat.get(bit_index)) <= t) {
                res.add(dat.get(bit_index));
            }
        }
    }

    public static int countTrue(boolean... values) {
        int count = 0;
        boolean[] arr$ = values;
        int len$ = values.length;

        for (int i$ = 0; i$ < len$; ++i$) {
            boolean value = arr$[i$];
            if (value) {
                ++count;
            }
        }

        return count;
    }

    //********************************* Debug code below here *********************************//

    private static void show(int[] array, String title) {
        System.out.print(title + "length: " + array.length + " = ");
        for (int i = 0; i < array.length; i++) {
            System.out.print(array[i] + " ");
        }
        System.out.println();
    }

    private static void show(double[] array, String title) {
        System.out.print(title + "length: " + array.length + " = ");
        for (int i = 0; i < array.length; i++) {
            System.out.print(array[i] + " ");
        }
        System.out.println();
    }

}
