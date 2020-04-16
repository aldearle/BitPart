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

import coreConcepts.Metric;

import java.util.*;

/**
 * Created by al on 06/09/2017.
 *
 * This should be in Metrics in Metric Framework package
 * TODO Move or delete.
 */
public class Jaccard implements Metric<String> {

    public double distance(Collection A, Collection B ) {
        return 1 - ( (double) ( intersection( A,B ).size() ) ) / union( A, B ).size();
    }

    public double distance(String A, String B ) {
        Collection agrams = Shingle.ngrams(A,2);
        Collection bgrams = Shingle.ngrams(B,2);
        return 1 - ( ( intersection( agrams,bgrams ).size() ) ) / union( agrams, bgrams ).size();
    }

    public double distance(OpenBitSet A, OpenBitSet B ) {

        double A_card = A.cardinality();
        double B_card = B.cardinality();
        if( A_card == 0 && B_card == 0 ) {
            return 0;
        }
        OpenBitSet intersection = A.get(0,A.length());  // and is a destructive operation - so copy.
        intersection.and(B);
        double intersection_card = intersection.cardinality();

        return 1 - ( intersection_card / ( A_card + B_card - intersection_card ) );

    }


    @Override
    public String getMetricName() {
        return "Jaccard";
    }


    public static Set union(Collection a, Collection b) {
        Set result = new HashSet();
        result.addAll(a);
        result.addAll(b);
        return result;
    }

    public static Set intersection(Collection a, Collection b) {
        Set result = new HashSet();
        Iterator a_iter = a.iterator();
        while( a_iter.hasNext() ) {
            Object next = a_iter.next();
            if( b.contains( next ) ) {
                result.add( next );
            }
        }
        return result;
    }


    public static void main( String[] args ) {
        OpenBitSet all_ones = new OpenBitSet();
        all_ones.set( 0,99, true );
        OpenBitSet all_zeros = all_ones.get(0,all_ones.length()); // a copy
        all_zeros.flip(0,all_zeros.length());
        all_zeros.set( 0,99, false );
        OpenBitSet random = new OpenBitSet();
        random.set( 0,99, false );
        random_fill( random );
        OpenBitSet flip_random = random.get(0,random.length()); // a copy
        flip_random.flip(0,flip_random.length());
        System.out.println( flip_random );
        Jaccard jacc = new Jaccard();

        System.out.println( " distance all_ones all_ones = " + jacc.distance( all_ones, all_ones ) );
        System.out.println( " distance all_zeros all_zeros = " + jacc.distance( all_zeros, all_zeros ) );
        System.out.println( " distance random random = " + jacc.distance( random, random ) );
        System.out.println( " distance all_ones random = " + jacc.distance( all_ones, random ) );
        System.out.println( " distance all_zeros random = " + jacc.distance( all_zeros, random ) );
        System.out.println( " distance all_ones flip_random = " + jacc.distance( all_ones, flip_random ) );
        System.out.println( " distance all_zeros flip_random = " + jacc.distance( all_zeros, flip_random ) );
        System.out.println( " distance random flip_random = " + jacc.distance( random, flip_random ) );

    }

    private static void random_fill(OpenBitSet bs) {
        Random r = new Random();
        for( int i = 0; i < 100; i++ ) {
            if( r.nextBoolean() ) {
                bs.set(i);
            }
        }
    }
}
