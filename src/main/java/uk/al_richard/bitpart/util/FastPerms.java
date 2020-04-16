package uk.al_richard.bitpart.util;

public class FastPerms {

    public static int[] getNthPair(int i) {
        int guess = (int) Math.floor(Math.sqrt(2 * i));
        int ch3 = ((guess + 1) * guess) / 2;
        if (ch3 == i) {
            return new int[]{ 0, guess + 1};
        } else if (ch3 < i) {
            return new int[]{ i - ch3, guess + 1};
        } else {
            return new int[]{ guess - (ch3 - i), guess};
        }
    }

    public static void main( String[] args ) {

        final int noOfRefPoints = 6;

        int nchoose2 = noOfRefPoints * (noOfRefPoints - 1)  / 2;

        for (int i = 0; i < nchoose2; i++) {
            int[] p = getNthPair( i );
            System.out.println( i + " " + p[0] + " " + p[1] );
        }
    }


}
