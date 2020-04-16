package uk.al_richard.bitpart.mutiThreaded;

class BlockResult<T> {
    public final int block_start_in_longs;
    public final long[] block_results;

    public BlockResult(int block_start_in_longs, long[] block_results ) {
        this.block_start_in_longs = block_start_in_longs;
        this.block_results = block_results;
    }
}