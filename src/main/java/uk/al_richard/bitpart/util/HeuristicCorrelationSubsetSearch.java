package uk.al_richard.bitpart.util;

import java.util.*;

/*
 * from Improving Sketches for similarity search:
 * @article{mic_improving_2015,
 * 	title = {Improving Sketches for Similarity Search},
 * 	pages = {13},
 * 	journaltitle = {Workshop on Mathematical and Engineering Methods in  Computer Science ({MEMICS}'15)},
 * 	author = {Mic, Vladimir and Novak, David and Zezula, Pavel},
 * 	date = {2015},
 * }
 *
 * Code from from v.mic@centrum.cz (XMIC) sf.pivotAnalyzer.uncorrelatedSelectorImpl.HeuristicCorrelationSubsetSearch
 * See https://www.fi.muni.cz/~xmic/sketches/impl/
 *
 */

public class HeuristicCorrelationSubsetSearch {

    private Random rand = new Random( 124356 );

    public List<Integer> selectLowCorrelatedSubset(OpenBitSet[] datarep, int bitsetsize, int required  ) {
        return selectLowCorrelatedSubset( createCorrelaionMatrix( datarep, bitsetsize ),required );
    }

    private double[][] createCorrelaionMatrix( OpenBitSet[] datarep, int bitsetsize ) {

        double[][] matrix = new double[datarep.length][datarep.length];

        for( int i = 0; i < datarep.length -1; i++ ) {
            for (int j = i + 1; j < datarep.length; j++) {
                matrix[i][j] = Math.abs(pearson(datarep[i], datarep[j], bitsetsize));
                matrix[j][i] = matrix[i][j];
            }
        }
        return matrix;
    }

    // Code down to **** from v.mic@centrum.cz (XMIC) sf.pivotAnalyzer.uncorrelatedSelectorImpl.HeuristicCorrelationSubsetSearch
    // https://www.fi.muni.cz/~xmic/sketches/impl/
    // Heavily edited by al

    private final int i = 4000;
    private final int k = 10;


    private List<Integer> selectLowCorrelatedSubset(double[][] corr, int required ) {

        List<Integer> ret = null;

        double min_max_corr = Double.MAX_VALUE;

        for( int starts = 0; starts < k; starts++ ) {

            List<Integer> curIndexes = new ArrayList<>();

            addIndexes(curIndexes, required, corr, false);

            for (int iterations = 0; iterations < i; iterations++) {

                boolean bySum = percentageProbability( 60 );

                double current_max_corr = getHighestCorrelation(curIndexes, corr);
                if (current_max_corr < min_max_corr) {
                    ret = new ArrayList<>(curIndexes);
                    min_max_corr = current_max_corr;
                }

                List<Integer> correlatedIndexes = getCorrelatedIndexes(current_max_corr, curIndexes, corr, 0.02);
                curIndexes.removeAll(correlatedIndexes);

                addIndexes(curIndexes, required, corr, bySum);
            }
        }

        return ret.subList(0,required);
    }

    private boolean percentageProbability(int prob) {
        return rand.nextInt(100) < prob;
    }


    private void addIndexes(Collection<Integer> curIndexes, int required, double[][] corr, boolean bySum) {

            while (curIndexes.size() < required) {
                double[] max_corr = calculateMaxCorr(curIndexes, corr, bySum);

                double minMaxCorSum = Double.MAX_VALUE;

                int addIdx = 0;

                for (int index = 0; index < max_corr.length; index++) {
                    double sum = 0;

                    if ( ! curIndexes.contains(index)) {
                        sum += max_corr[index];
                        if( sum < minMaxCorSum) {
                            addIdx = index;
                            minMaxCorSum = sum;
                        }
                    }
                }
                curIndexes.add(addIdx);
            }
        }

    private double[] calculateMaxCorr(Collection<Integer> curIndexes, double[][] corr, boolean bySum) {
        int start = rand.nextInt(corr.length);
        double[] max_corr =  new double[corr.length];
        for (int i = 0; i < corr.length; i++) {
            int idx = (start + i) % corr.length;
            max_corr[idx] = -1;
            if (curIndexes.contains(idx)) {
                max_corr[idx] = 1;
            } else {
                if (bySum) {
                    double sum = 0;
                    for (int intSet : curIndexes) {
                        sum += corr[idx][intSet];
                    }
                    max_corr[idx] = sum;
                } else {
                    for (int intSet : curIndexes) {
                        if (corr[idx][intSet] > max_corr[idx]) {
                            max_corr[idx] = corr[idx][intSet];
                        }
                    }
                }
            }
        }
        return max_corr;
    }


    /**
     *
     * @param curIndexes - the current indices in the matrix
     * @param corr - the pearson correlation matrix
     * @return the highest correlation in the matrix chosen from the curIndexes.
     */
    private double getHighestCorrelation(List<Integer> curIndexes, double[][] corr) {
        double result = 0;
        for (int i = 0; i < curIndexes.size() - 1; i++) {
            int idxI = curIndexes.get( i );
            for (int j = i + 1; j < curIndexes.size(); j++) {
                int idxJ = curIndexes.get( j );
                if (corr[idxI][idxJ] > result) {
                    result = corr[idxI][idxJ];
                }
            }
        }
        return result;
    }

    /**
     *
     * @param worstCorrelation - indices worse than this are being removed
     * @param diff - the difference tolerated in the worstCorrelation
     * @param curIndexes - the set of active indices
     * @param corr - the correlation matrix
     * @return the list of indices whose correlation is greater than (worstCorrelation - diff)
     */
    private List<Integer> getCorrelatedIndexes(double worstCorrelation, List<Integer> curIndexes, double[][] corr, double diff) {
        List<Integer> removed = new ArrayList<>();
        double limit = worstCorrelation - diff;
        for (int i = 0; i < curIndexes.size() - 1; i++) {
            int idxI = curIndexes.get(i);
            for (int j = i + 1; j < curIndexes.size(); j++) {
                int idxJ = curIndexes.get(j);
                if (corr[idxI][idxJ] > limit && !removed.contains(idxI) && !removed.contains(idxJ)) {
                    double sumI = 0;
                    double sumJ = 0;
                    for (int k = 0; k < curIndexes.size(); k++) {
                        sumI += corr[idxI][k];
                        sumJ += corr[idxJ][k];
                    }
                    int rem = sumI > sumJ ? idxI : idxJ;
                    removed.add(rem);
                }
            }
        }
        return removed;
    }

    // **** end of code from v.mic@centrum.cz

    public double pearson( OpenBitSet s, OpenBitSet t, int bitsetsize ) {

        OpenBitSet product = (OpenBitSet) s.clone();
        product.and(t);

	    double top = bitsetsize * product.cardinality() - ( s.cardinality() * t.cardinality() );
        double left = thing(bitsetsize, s);
        double right = thing(bitsetsize, t);
        double bottom = left * right;

        return top / bottom;
    }

    private double thing(int n, OpenBitSet s) {
        int s_card = s.cardinality();
        return Math.sqrt( n * s_card - ( s_card * s_card ) );
    }

    public void main( String[] args ) {
        OpenBitSet A = new OpenBitSet(10);
        A.set( 0,5 );

        OpenBitSet B = new OpenBitSet(10);
        B.set( 5,10 );

        System.out.println( pearson( A,B, 10 ) );
    }


}
