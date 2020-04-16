package uk.al_richard.bitpart.util;

import java.util.ArrayList;
import java.util.List;

public class Permutations {

    private List<Integer> triangular_numbers = new ArrayList<>();          // keeps a lazily generated cache of triangular numbers
    private int[][] pairs;                                                 // cache of generated

    public Permutations( int num_perms ) {
        pairs = new int[num_perms][];

        for (int i = 0; i < num_perms; i++) {
            pairs[i] = nthPair( i );
        }
    }

    public int[][] getPairs() {
        return pairs;
    }

    public int[] getNthPair( int i ) {
        return pairs[i];
    }

    private int triangle( int n ) {
        if( n > triangular_numbers.size() ) {
            generate_triangles( triangular_numbers.size() + 1, n );
        }
        return triangular_numbers.get(n);
//        return n * (n + 1 ) / 2;
    }

    private int isTriangluar(int n) {
        if( n > triangular_numbers.size() ) {
            generate_triangles( triangular_numbers.size() + 1, n );
        }
        int res = triangular_numbers.indexOf(n);
        if( res == -1 ) {
            return -1;
        } else {
            return res + 1;
        }
    }

    private void generate_triangles( int start, int end ) {
        for (int i = start; i <= end; i++) {
            triangular_numbers.add( i * (i + 1 ) / 2 ); // the ith triangular number
        }
    }

    private int[] nthPair( int i ) {

        int[] cached = pairs[i];
        if( cached != null ) {
            return cached;
        }
        if (i == 0) {
            return new int[] {0, 1};
        } else {
            int m = isTriangluar(i);
            if (m != -1) {
                int[] result = new int[]{0, m + 1};
                pairs[i] = result;
                return result;
            } else {
                int[] p = nthPair(i - 1);
                int[] result = new int[] {p[0] + 1, p[1]};
                pairs[i] = result;
                return result;
            }
        }
    }


    public static void main( String[] args ) {

        final int noOfRefPoints = 6;

        int nchoose2 = noOfRefPoints * (noOfRefPoints - 1)  / 2;

        Permutations p = new Permutations( nchoose2 );

        for (int i = 0; i < nchoose2; i++) {
            int[] pair = p.getNthPair( i );
            System.out.println( i + " " + pair[0] + " " + pair[1] );
        }
    }

}
