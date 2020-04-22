package uk.al_richard.bitpart.mutiThreaded;

import uk.richardconnor.metricSpaceFramework.coreConcepts.CountedMetric;
import uk.richardconnor.metricSpaceFramework.coreConcepts.DataDistance;
import uk.richardconnor.metricSpaceFramework.coreConcepts.Metric;
import uk.al_richard.bitpart.referenceImplementation.ExclusionZone;
import uk.al_richard.bitpart.referenceImplementation.RefPointSet;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static uk.al_richard.bitpart.util.BitUtils.bits_to_longs;

public class BlockSlice<T> extends Thread {


    private boolean running;
    private final SynchronousQueue<RunnableFuture> queue;
    private final int datasize;
    private final int degree_of_parallelism;
    private final int final_blocksize_in_longs;
    private final int final_blocksize_in_bits;
    public final int final_block_start_in_longs;
    private final int final_block_start_in_bits;
    private long[][] datarep; // rows are all noOfBitSets (in bits) columns in size, each array row is the bits 0..64 of the rows, all initialised to 0L (by default)
    private final List<T> data;
    private final RefPointSet<T> rps;
    private final List<ExclusionZone<T>> ezs;

    public BlockSlice(List<T> data, RefPointSet<T> rps, List<ExclusionZone<T>> ezs, int datasize, int degree_of_parallelism, int block, int noOfBitSets ) {

        this.data = data;
        this.rps = rps;
        this.ezs = ezs;
        this.datasize = datasize;
        this.degree_of_parallelism = degree_of_parallelism;

        queue = new SynchronousQueue<>();
        running = true;

        final int dataSize_in_longs = bits_to_longs( datasize );

        int blocksize_in_longs = Math.max( (int) Math.ceil( dataSize_in_longs / ( (double) degree_of_parallelism ) ), 1); // protect when data size is < degree_of_parallelism
        if( dataSize_in_longs % blocksize_in_longs !=0 ) {  // round up if non integral value.
            blocksize_in_longs += 1;
        }

        final_blocksize_in_longs = blocksize_in_longs;
        final_blocksize_in_bits = blocksize_in_longs * Long.SIZE;

        final_block_start_in_longs = block * final_blocksize_in_longs;
        final_block_start_in_bits = final_block_start_in_longs * Long.SIZE;

//        show_block( block, final_blocksize_in_longs, final_blocksize_in_bits, final_block_start_in_longs, final_block_start_in_bits );

        datarep = new long[ final_blocksize_in_longs ][ noOfBitSets ]; // rounds up to ensure capacity - potentially slightly too big.
    }

    private void show_block(int block, int final_blocksize_in_longs, int final_blocksize_in_bits, int final_block_start_in_longs, int final_block_start_in_bits) {
        System.out.println( "Block = " + block );
        System.out.println( "Size longs = " + final_blocksize_in_longs );
        System.out.println( "size bits = " + final_blocksize_in_bits );
        System.out.println( "start longs = " + final_block_start_in_longs );
        System.out.println( "start bits = " + final_block_start_in_bits );
    }

    public BlockSlice(List<T> data, RefPointSet<T> rps, List<ExclusionZone<T>> ezs, int datasize, int degree_of_parallelism, int block, int noOfBitSets, int[] ez_in_counts, int[] ez_out_counts ) {

        this.data = data;
        this.rps = rps;
        this.ezs = ezs;
        this.datasize = datasize;
        this.degree_of_parallelism = degree_of_parallelism;

        queue = new SynchronousQueue<>();
        running = true;

        final int dataSize_in_longs = bits_to_longs( datasize );

        int blocksize_in_longs = dataSize_in_longs / degree_of_parallelism;
        if( dataSize_in_longs % blocksize_in_longs !=0 ) {  // round up if non integral value.
            blocksize_in_longs += 1;
        }

        final_blocksize_in_longs = blocksize_in_longs;
        final_blocksize_in_bits = blocksize_in_longs * Long.SIZE;

        final_block_start_in_longs = block * final_blocksize_in_longs;
        final_block_start_in_bits = final_block_start_in_longs * Long.SIZE;

        datarep = new long[ final_blocksize_in_longs ][ noOfBitSets ]; // rounds up to ensure capacity - potentially slightly too big.

    }

    public void run() {
        while( running ) {
            try {
                RunnableFuture rf = queue.take();
                rf.run();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void terminate() {
        try {
            queue.put(new FutureTask( new Callable<Object>() {
                @Override
                public Object call() throws Exception {

                    running = false;
                    return null;
                }
            } ) );
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public Future<Object> buildSliceBitSetData() {

        RunnableFuture rf = new FutureTask( new BuildBitData() );
        try {
            queue.put(rf);
            return rf;
        } catch (InterruptedException  e) {
            e.printStackTrace();
            return null;
        }
    }

    private class BuildBitData implements Callable {

        @Override
        public Object call() throws Exception {

            for (int bit_row = 0; bit_row < final_blocksize_in_bits ; bit_row++) {
                int source_index = bit_row + final_block_start_in_bits;
                if( source_index < data.size() ) {  // might be bigger due to block rounding of longs
                    T p = data.get(source_index);
                    double[] dists = rps.extDists(p);
                    for (int ez_index = 0; ez_index < ezs.size(); ez_index++) {
                        boolean isIn = ezs.get(ez_index).isIn(dists);
                        if (isIn) {
                            setbit(datarep, bit_row, ez_index);
                        }
                    }
                }
            }
            return null;
        }

        private static final int MASK = 63;

        private void setbit(long[][] datarep, int bit_row, int bit_column) {
            /*
             * Assumes this is setting bit row, column to 1.
             * If we need to set to zero as well need a target &= (ALL_ONES - bit_row %  Long.SIZE);
             */

            int long_row_index = bit_row / Long.SIZE;
            long target = datarep[long_row_index][bit_column];
            target |= 1l << ( bit_row %  Long.SIZE );
            datarep[long_row_index][bit_column] = target;
        }
    }



    public Future<List<T>> querySlice(double threshold, Metric<T> metric, T q, List<Integer> mustBeIn, List<Integer> cantBeIn ) throws InterruptedException {
        RunnableFuture<List<T>> rf = new FutureTask( new BlockExclusions( threshold, metric, q, mustBeIn, cantBeIn ) );
        queue.put(rf);
        return rf;
    }

    private class BlockExclusions implements Callable<List<DataDistance<T>>> {

        private final double threshold;
        private final Metric<T> metric;
        private final T q;
        private final List<Integer> mustBeIn;
        private final List<Integer> cantBeIn;

        public BlockExclusions(double threshold, Metric<T> metric, T q, List<Integer> mustBeIn, List<Integer> cantBeIn ) {
            this.threshold = threshold;
            this.metric = metric;
            this.q = q;
            this.mustBeIn = mustBeIn;
            this.cantBeIn = cantBeIn;
        }

        @Override
        public List<DataDistance<T>> call() throws Exception {

            long t0 = System.currentTimeMillis();
            final long[] block_results = new long[final_blocksize_in_longs]; // rounds up to ensure capacity - potentially slightly too big.

            for (int long_in_block = 0; long_in_block < final_blocksize_in_longs; long_in_block++) { // iterate over the rows (of longs) in the block

                long result = 0l;
                boolean block_uninitialised = true; // has this block been assigned yet? - one block_uninitialised per block and per thread - so thread safe

                long[] long_row = datarep[long_in_block];

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
                if (block_uninitialised) {  //  not excluded or included - don'threshold know case - must be conservative and include.
                    result = -1l; // set all bits to 1
                }
                block_results[long_in_block] = result;
            }
            return filter(block_results);
        }

        public List<DataDistance<T>> filter(long[] block_results ) {
            CountedMetric<T> cm = new CountedMetric<>(metric);
            List<DataDistance<T>> list_results = new ArrayList<>();

            for (int long_in_block = 0; long_in_block < block_results.length; long_in_block++) {
                long long_bits = block_results[long_in_block];
                for (int bit_in_long = 0; bit_in_long < Long.SIZE; bit_in_long++) {
                    int bit_index = final_block_start_in_bits + ( long_in_block * Long.SIZE ) + bit_in_long;
                    if (bit_index < data.size() && ((long_bits & (1l << bit_in_long)) != 0) /* ith bit set */ ) {
                        double distance = cm.distance(q, data.get(bit_index));
                        if( distance <= threshold ) {
                            list_results.add( new DataDistance<T>(data.get(bit_index),distance));
                        }
                    }
                }
            }
            return list_results;
        }
    }

}
