/*
 * Copyright 2018 Systems Research Group, University of St Andrews:
 * <https://github.com/stacs-srg>
 *
 * This file is part of the module utilities.
 *
 * utilities is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * utilities is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with utilities. If not, see
 * <http://www.gnu.org/licenses/>.
 */

package uk.al_richard.bitpart.util;


import uk.richardconnor.metricSpaceFramework.coreConcepts.Metric;

import java.util.BitSet;

// This is effectively inverse of HammingDistance.
public class HammingSimilarity implements Metric<BitSet> {

    @Override
    public double distance(BitSet x, BitSet y) {
        return similarity(x,y);
    }

    /**
     *
     * @param x - a bitset over which to caclulate HammingDistance distance
     * @param y - a bitset over which to caclulate HammingDistance distance
     * @return - return the normalised HammingDistance distance - if all bits are different return 1, if all the same return 0
     */
    public double hammingDistance(BitSet x, BitSet y) {

        long[] x_as_longs = x.toLongArray();    // these arrays can miss out trailing zeros
        long[] y_as_longs = y.toLongArray();    // therefore the two arrays can be different lengths
                                                // Strange code below guards against this.

        int max = Math.max( x_as_longs.length, y_as_longs.length );

        if( max == 0 ) {
            return 0;  // to avoid NaN in division below.
        }

        double count = 0;

        for( int i = 0; i < max; i++ ) {

            long xi = i < x_as_longs.length ? x_as_longs[i] : 0;
            long yi = i < y_as_longs.length ? y_as_longs[i] : 0;

            long d = distance( xi,yi );
            count += d;
        }
        return count / ( max * Long.SIZE );
    }

    /**
     *
     * @param x - a bitset over which to caclulate HammingDistance distance
     * @param y - a bitset over which to caclulate HammingDistance distance
     * @return - return the normalised HammingDistance similarity - if all bits are different return 0, if all the same return 1
     */
    public double similarity(BitSet x, BitSet y) {
        return 1 - hammingDistance(x,y);
    }


    public int distance(long x, long y) {
        if( x == 0 && y == 0 ) {
            return 0;
        }
        int count = 0;
        while (x > 0 || y > 0) { // keep going until all zeros.
            if (x % 2 != y % 2) {
                count++; // remainders are different
            }
            x = x / 2;  // shift fill with 0
            y = y / 2;  // shift fill with 0
        }
        return count;
    }

    @Override
    public String getMetricName() {
        return "HammingDistance";
    }
}
