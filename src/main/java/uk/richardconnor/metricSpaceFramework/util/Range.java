package uk.richardconnor.metricSpaceFramework.util;

import java.util.Iterator;

public class Range implements Iterable<Integer> {

    Iterator<Integer> i;

    /**
     * @param lwb
     *            start of range (inclusive)
     * @param upb
     *            end of range (exclusive)
     *
     *            returns an Iterable&lt;Integer&gt; object which returns integer
     *            values from lwb (inclusive) to upb (exclusive) ; just
     *            syntactic sugar to avoid the now-horrible use of for loops
     *            over integers!
     */
    public Range(final int lwb, final int upb) {
        final int[] box = { lwb };
        this.i = new Iterator<Integer>() {
            @Override
            public boolean hasNext() {
                return box[0] < upb;
            }

            @SuppressWarnings("boxing")
            @Override
            public Integer next() {
                return box[0]++;
            }
        };
    }

    public static Range range(int lwb, int upb) {
        return new Range(lwb, upb);
    }

    @Override
    public Iterator<Integer> iterator() {
        return this.i;
    }
}
