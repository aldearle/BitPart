package uk.al_richard.bitpart.mutiThreaded;

import uk.al_richard.bitpart.referenceImplementation.ExclusionZone;
import uk.al_richard.bitpart.referenceImplementation.RefPointSet;
import uk.al_richard.bitpart.util.FastPerms;
import uk.al_richard.bitpart.util.OpenBitSet;
import uk.richardconnor.metricSpaceFramework.coreConcepts.CountedMetric;
import uk.richardconnor.metricSpaceFramework.coreConcepts.Metric;
import uk.richardconnor.metricSpaceFramework.datapoints.CartesianPoint;
import uk.richardconnor.metricSpaceFramework.testloads.TestContext;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import static java.util.concurrent.Executors.newFixedThreadPool;
import static uk.al_richard.bitpart.util.BitUtils.bits_to_longs;


/**
 * First version of Multi threaded Bitblaster (hence the MT).
 * Uses OpenBitSet for data stored column wise.
 * 3 phases multi threaded, results collected in ConcurrentLinkedQueue.
 * All phases synchronised using latches.
 * Single result vector in phase 2.
 *
 * @author al dearle (al@st-andrews.ac.uk)
 *
 */
public class MainRunnerFileMT extends MainRunnerFile {

    public static void main(String[] args) throws InterruptedException, ExecutionException {

        TestContext.Context context = TestContext.Context.euc20;

        int noOfRefPoints = 50;
        int nChoose2 = ((noOfRefPoints - 1) * noOfRefPoints) / 2;
        int noOfBitSets = nChoose2 + (noOfRefPoints * ball_radii.length);

        int degree_of_parallelism = 2; //Runtime.getRuntime().availableProcessors();
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
            ConcurrentLinkedQueue<T> res = new ConcurrentLinkedQueue<>();
            double[] dists = rps.extDists(q, res, threshold);

            boolean[] mustBeIn = new boolean[noOfBitSets];
            boolean[] cantBeIn = new boolean[noOfBitSets];

            long t1 = System.currentTimeMillis();
            findExclusionsMT(dists, threshold, mustBeIn, cantBeIn, ezs, numberOfSheets, global_executor, degree_of_parallelism );
            long t2 = System.currentTimeMillis();

            int cantbein_size = countTrue( cantBeIn );
            int mustbein_size = countTrue( mustBeIn );

            partitionsExcluded += cantbein_size + mustbein_size;

            long t3 = System.currentTimeMillis();

            OpenBitSet inclusions = doExclusionsMT(datarep, data.size(), ezs.size(), mustBeIn, mustbein_size, cantBeIn, cantbein_size, global_executor, degree_of_parallelism );

            long t4 = System.currentTimeMillis();
            filterContendersMT(data, threshold, cm, q, res, data.size(), inclusions, global_executor, degree_of_parallelism );
            long t5 = System.currentTimeMillis();

            find_exclusions_time += t2 - t1;
            do_exclusions_time += t4 - t3;
            filter_time += t5 - t4;

            noOfResults += res.size();
        }

        long total_time = (System.currentTimeMillis() - t0);

        System.out.println("bitsets");
        System.out.println("dists per query\t"
                + (cm.reset() / queries.size() + noOfRefPoints));
        System.out.println("results\t" + noOfResults);
        System.out.println("time\t" + total_time / (float) queries.size());
        System.out.println("find_exclusions_time\t" + find_exclusions_time / (float) queries.size() + " " + find_exclusions_time * 100 / total_time + "%" );
        System.out.println("do_exclusions_time\t" + do_exclusions_time  / (float) queries.size() + " " + do_exclusions_time * 100 / total_time + "%" );
        System.out.println("filter_time\t" + filter_time  / (float) queries.size() + " " + filter_time * 100 / total_time + "%" );
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




    static <T> void filterContendersMT(List<T> dat, double t,
                                     CountedMetric<T> cm, T q, ConcurrentLinkedQueue<T> res, final int dataSize,
                                     OpenBitSet results, final ExecutorService global_executor, final int degree_of_parallelism ) throws InterruptedException, ExecutionException {

        final int results_size = results.size();

        int blocksize = results_size / degree_of_parallelism;
        if( results_size % degree_of_parallelism !=0 ) {  // round up if non integral value.
            blocksize += 1;
        }

        final int final_blocksize = blocksize;
        final int number_blocks = degree_of_parallelism; // keep this as a separate variable - may change this!

        //ExecutorService filter_executor = newFixedThreadPool(degree_of_parallelism);
        final CountDownLatch latch = new CountDownLatch(degree_of_parallelism);
        //List<Future> futures = new ArrayList<Future>();

        for (int block = 0; block < number_blocks; block++) {  // spark off number_blocks parallel computations

            final int block_start = block * final_blocksize;

            //futures.add(global_executor.submit(() -> {
            global_executor.execute(() -> {

                for (int i = results.nextSetBit(block_start); i != -1 && i < block_start + final_blocksize && i < dat.size() /* needed due to block rounding */ ; i = results.nextSetBit(i + 1)) {
                    if( results.get(i)) {
                        if (cm.distance(q, dat.get(i)) <= t) {
                            res.add(dat.get(i));
                        }
                    }
                }
                latch.countDown();
            });
        }
        latch.await();
        //for(Future f: futures) { f.get(); } // wait for all tasks to complete
        //filter_executor.shutdown();
        //filter_executor.awaitTermination(50l, TimeUnit.SECONDS);
    }

    private static <T> void findExclusionsMT(final double[] dists, final double threshold, final boolean[] mustBeIn, final boolean[] cantBeIn, final List<ExclusionZone<T>> ezs, final int numberOfSheets, final ExecutorService global_executor, final int degree_of_parallelism) throws InterruptedException, ExecutionException {

        final int ezs_size = ezs.size();

        int blocksize = ezs_size / degree_of_parallelism;
        if( ezs_size % degree_of_parallelism !=0 ) {  // round up if non integral value.
            blocksize += 1;
        }

        final int final_blocksize = blocksize;
        final int number_blocks = degree_of_parallelism; // keep this as a separate variable - may change this!

        //ExecutorService find_executor = newFixedThreadPool(degree_of_parallelism);
        final CountDownLatch latch = new CountDownLatch(degree_of_parallelism);
        //List<Future> futures = new ArrayList<Future>();

        for (int block = 0; block < number_blocks; block++) {  // spark off number_blocks parallel computations

            final int block_start = block * final_blocksize;

            //futures.add(global_executor.submit(() -> {
            global_executor.execute(() -> {
                for (int i = 0; i < final_blocksize; i++) {
                    int next_ez = block_start + i;
                    if (next_ez < ezs_size) {
                        mustBeInOrOut(next_ez, threshold, numberOfSheets, MainRunnerFile.ball_radii, dists, mustBeIn, cantBeIn);
                    }
                }
                latch.countDown();
            });
        }
        latch.await();
        //for(Future f: futures) { f.get(); } // wait for all tasks to complete
        //find_executor.shutdown();
        //find_executor.awaitTermination(50l, TimeUnit.SECONDS);
    }

    protected static <T> OpenBitSet doExclusionsMT( final OpenBitSet[] datarep, final int dataSize, final int ezsSize, final boolean[] mustBeIn, final int mustbein_size, final boolean[] cantBeIn, final int cantbein_size, final ExecutorService global_executor, final int degree_of_parallelism ) throws InterruptedException, ExecutionException {

        int dataSize_in_longs = bits_to_longs( dataSize );
        int blocksize_in_longs = dataSize_in_longs / degree_of_parallelism;
        if( dataSize_in_longs % blocksize_in_longs !=0 ) {  // round up if non integral value.
            blocksize_in_longs += 1;
        }

        final int final_blocksize_in_longs = blocksize_in_longs;
        final int number_blocks = degree_of_parallelism; // keep this as a separate variable - may change this!
        final long[] results = new long[ dataSize_in_longs ]; // rounds up to ensure capacity - potentially slightly too big.

        //ExecutorService do_executor = newFixedThreadPool(degree_of_parallelism);
        final CountDownLatch latch = new CountDownLatch(degree_of_parallelism);
        //List<Future> futures = new ArrayList<Future>();

        for (int block = 0; block < number_blocks; block++) {  // spark off number_blocks parallel computations

            final int block_start = block * blocksize_in_longs;

            global_executor.execute(() -> {
            //futures.add(global_executor.submit(() -> {

                // Each thread makes blocksize_in_longs assignments to the results bitset.
                // Last one may not due to rounding.
                // if block_uninitialised remains true at end of column tests (in for loop) we are conservative and set all bits in block to 1 (-true).

                boolean block_uninitialised = true; // has this block been assigned yet? - one block_uninitialised per block and per thread - so thread safe

                for (int column_id = 0; column_id < ezsSize; column_id++) { // Test inclusion and exclusion for all columns

                    if (mustBeIn[column_id]) {
                        if (block_uninitialised) {
                            for (int long_in_block = 0; long_in_block < final_blocksize_in_longs; long_in_block++) {
                                if (block_start + long_in_block < dataSize_in_longs) {
                                    results[block_start + long_in_block] = datarep[column_id].getLong(block_start + long_in_block);
                                }
                            }
                            block_uninitialised = false;
                        } else {
                            for (int long_in_block = 0; long_in_block < final_blocksize_in_longs; long_in_block++) {
                                if (block_start + long_in_block < dataSize_in_longs) {
                                    results[block_start + long_in_block] &= datarep[column_id].getLong(block_start + long_in_block);
                                }
                            }
                        }
                    }
                    if (cantBeIn[column_id]) {
                        if (block_uninitialised) {
                            for (int long_in_block = 0; long_in_block < final_blocksize_in_longs; long_in_block++) {
                                if (block_start + long_in_block < dataSize_in_longs) {
                                    results[block_start + long_in_block] = ~datarep[column_id].getLong(block_start + long_in_block);
                                }
                            }
                            block_uninitialised = false;
                        } else {
                            // and not:
                            for (int long_in_block = 0; long_in_block < final_blocksize_in_longs; long_in_block++) {
                                if (block_start + long_in_block < dataSize_in_longs) {
                                    results[block_start + long_in_block] &= ~datarep[column_id].getLong(block_start + long_in_block);
                                }
                            }
                        }
                    }
                    if (block_uninitialised) {  //  not excluded or included - don't know case - must be conservative and include.
                        for (int long_in_block = 0; long_in_block < final_blocksize_in_longs; long_in_block++) {
                            if (block_start + long_in_block < dataSize_in_longs) {
                                results[block_start + long_in_block] = -1l; // set all bits to 1
                            }
                        }
                    }
                }
                latch.countDown();
            });
        }
        latch.await();
        //for(Future f: futures) { f.get(); } // wait for all tasks to complete
        //do_executor.shutdown();
        //do_executor.awaitTermination(50l, TimeUnit.SECONDS);
        return OpenBitSet.valueOf(results);
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
