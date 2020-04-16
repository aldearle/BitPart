package uk.al_richard.bitpart.production;

import coreConcepts.CountedMetric;
import coreConcepts.Metric;
import uk.ac.standrews.cs.utilities.metrics.coreConcepts.DataDistance;
import uk.al_richard.bitpart.referenceImplementation.*;
import uk.al_richard.bitpart.util.OpenBitSet;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

public class MetricBitBlaster<T> {

    private final RefPointSet<T> rps;
    private List<T> dat;
    private OpenBitSet[] datarep;
    private BiFunction<T,T,Double> distfn;
    private CountedMetric<T> cm;

    private final int nChoose2;
    private final int noOfBitSets;
    private final int number_sheets;
    private final int number_of_balls;
    private List<ExclusionZone<T>> ezs;
    private int number_of_items;    // Keep a note of how many items are in the data set
    private boolean fourPoint = false;
    private boolean balanced = false;

    private static final double RADIUS_INCREMENT = 0.3;                 // TODO revisit these
    private static double MEAN_DIST = 1.81;                             // TODO revisit these
    private static double[] ball_radii = new double[] {
            MEAN_DIST - 2 * RADIUS_INCREMENT, MEAN_DIST - RADIUS_INCREMENT,
            MEAN_DIST, MEAN_DIST - RADIUS_INCREMENT,
            MEAN_DIST - 2 * RADIUS_INCREMENT };


    public MetricBitBlaster(BiFunction<T,T,Double> distfn, List<T> refs, List<T> dat, boolean fourPoint, boolean balanced ) {

        this.distfn = distfn;
        this.dat = dat;
        this.fourPoint = fourPoint;
        this.balanced = balanced;

        Metric<T> m = new Metric<T>() {

            @Override
            public double distance(T o1, T o2) {
                return distfn.apply(o1,o2);
            }

            @Override
            public String getMetricName() {
                return "supplied";
            }
        };

        cm = new CountedMetric( m );


        rps = new RefPointSet<T>(refs, m);
        ezs = new ArrayList<>();

        int noOfRefPoints = refs.size();

        nChoose2 = ((noOfRefPoints - 1) * noOfRefPoints) / 2;
        noOfBitSets = nChoose2 + (noOfRefPoints * ball_radii.length);
        number_sheets = nChoose2;
        number_of_balls = noOfRefPoints * ball_radii.length;

        addSheetExclusions(dat, refs, rps, ezs);
        addBallExclusions(dat, refs, rps, ezs);

        datarep = new OpenBitSet[noOfBitSets];
        buildBitSetData(dat, datarep, rps, ezs);
    }


    /**
     * Find the nodes within range r of query.
     *
     * @param q - some data for which to find the neighbours within distance r
     * @param threshold     the distance from query over which to search
     * @return all those nodes within r of @param T.
     */
    public List<DataDistance<T>> rangeSearch(final T q, final double threshold) {
            List<DataDistance<T>> res = new ArrayList<>();

            double[] dists = rps.extDists(q);

            List<Integer> mustBeIn = new ArrayList<>();
            List<Integer> cantBeIn = new ArrayList<>();

            for (int i = 0; i < ezs.size(); i++) {
                ExclusionZone<T> ez = ezs.get(i);
                if (ez.mustBeIn( dists, threshold)) {
                    mustBeIn.add(i);
                } else if (ez.mustBeOut( dists, threshold)) {
                    cantBeIn.add(i);
                }
            }

            doExclusions(dat, threshold, datarep, cm, q, res, dat.size(), mustBeIn, cantBeIn);

            return res;
    }



    public int size() {
        return dat.size();
    }

    //--------- Private ---------

    private void addBallExclusions(List<T> dat,
                                   List<T> refs, RefPointSet<T> rps,
                                   List<ExclusionZone<T>> ezs) {
        for (int i = 0; i < refs.size(); i++) {
            List<BallExclusion<T>> balls = new ArrayList<>();
            for (double radius : ball_radii) {
                BallExclusion<T> be = new BallExclusion<>(rps, i,
                        radius);
                balls.add(be);
            }
            if (balanced) {
                balls.get(0).setWitnesses(dat.subList(0, 1000));
                double midRadius = balls.get(0).getRadius();
                double thisRadius = midRadius
                        - ((balls.size() / 2) * RADIUS_INCREMENT);
                for (int ball = 0; ball < balls.size(); ball++) {
                    balls.get(ball).setRadius(thisRadius);
                    thisRadius += RADIUS_INCREMENT;
                }
            }
            ezs.addAll(balls);
        }
    }

    private void addSheetExclusions(List<T> dat,
                                          List<T> refs, RefPointSet<T> rps,
                                          List<ExclusionZone<T>> ezs) {
        for (int i = 0; i < refs.size() - 1; i++) {
            for (int j = i + 1; j < refs.size(); j++) {
                SheetExclusion<T> se;
                if( fourPoint ) {
                    se = new SheetExclusion4p<>(rps, i, j);
                } else {
                    se = new SheetExclusion3p<>(rps, i, j);
                }
                if (balanced) {
                    se.setWitnesses(dat.subList(0, 1000));
                }
                ezs.add(se);
            }
        }
    }

    private static <T> void buildBitSetData(List<T> data, OpenBitSet[] datarep,
                                            RefPointSet<T> rps, List<ExclusionZone<T>> ezs) {
        int dataSize = data.size();
        for (int i = 0; i < datarep.length; i++) {
            datarep[i] = new OpenBitSet(dataSize);
        }
        for (int n = 0; n < dataSize; n++) {
            T p = data.get(n);
            double[] dists = rps.extDists(p);
            for (int x = 0; x < datarep.length; x++) {
                boolean isIn = ezs.get(x).isIn(dists);
                if (isIn) {
                    datarep[x].set(n);
                }
            }
        }
    }

    @SuppressWarnings("boxing")
    private static OpenBitSet getAndOpenBitSets(OpenBitSet[] datarep, final int dataSize,
                                        List<Integer> mustBeIn) {
        OpenBitSet ands = null;
        if (mustBeIn.size() != 0) {
            ands = datarep[mustBeIn.get(0)].get(0, dataSize);
            for (int i = 1; i < mustBeIn.size(); i++) {
                ands.and(datarep[mustBeIn.get(i)]);
            }
        }
        return ands;
    }

    @SuppressWarnings("boxing")
    private static OpenBitSet getOrOpenBitSets(OpenBitSet[] datarep, final int dataSize,
                                       List<Integer> cantBeIn) {
        OpenBitSet nots = null;
        if (cantBeIn.size() != 0) {
            nots = datarep[cantBeIn.get(0)].get(0, dataSize);
            for (int i = 1; i < cantBeIn.size(); i++) {
                final OpenBitSet nextNot = datarep[cantBeIn.get(i)];
                nots.or(nextNot);
            }
        }
        return nots;
    }

    private static <T> void doExclusions(List<T> dat, double t,
                                           OpenBitSet[] datarep, CountedMetric<T> cm, T q, List<DataDistance<T>> res,
                                           final int dataSize, List<Integer> mustBeIn, List<Integer> cantBeIn) {
        if (mustBeIn.size() != 0) {
            OpenBitSet ands = getAndOpenBitSets(datarep, dataSize, mustBeIn);
            if (cantBeIn.size() != 0) {
                /*
                 * hopefully the normal situation or we're in trouble!
                 */
                OpenBitSet nots = getOrOpenBitSets(datarep, dataSize, cantBeIn);
                nots.flip(0, dataSize);
                ands.and(nots);
                filterContenders(dat, t, cm, q, res, dataSize, ands);
            } else {
                // there are no cantBeIn partitions
                filterContenders(dat, t, cm, q, res, dataSize, ands);
            }
        } else {
            // there are no mustBeIn partitions
            if (cantBeIn.size() != 0) {
                OpenBitSet nots = getOrOpenBitSets(datarep, dataSize, cantBeIn);
                nots.flip(0, dataSize);
                filterContenders(dat, t, cm, q, res, dataSize, nots);
            } else {
                // there are no exclusions at all...
                for (T d : dat) {
                    double dist = cm.distance(q, d);
                    if ( dist < t) {
                        res.add(new DataDistance<T>(d,(float)dist));
                    }
                }
            }
        }
    }

    static <T> void filterContenders(List<T> dat, double t,
                                     CountedMetric<T> cm, T q, List<DataDistance<T>> res, final int dataSize,
                                     OpenBitSet results) {

//		System.out.println( "Contenders size = " + results.cardinality());

        for (int i = results.nextSetBit(0); i != -1 && i < dataSize; i = results.nextSetBit(i + 1)) {
            // added i < dataSize since Cuda code overruns datasize in the - don't know case - must be conservative and include, easier than complex bit masking and shifting.
            if (results.get(i)) {
                double dist = cm.distance(q, dat.get(i));
                if ( dist <= t) {
                    res.add(new DataDistance<T>(dat.get(i),(float) dist));   // TODO change this test in all?
                }
            }
        }
    }

}
