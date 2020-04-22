package uk.richardconnor.metricSpaceFramework.generators;

import uk.richardconnor.metricSpaceFramework.coreConcepts.DataSet;
import uk.richardconnor.metricSpaceFramework.datapoints.CartesianPoint;

import java.util.Iterator;
import java.util.Random;


public class CartesianPointGenerator implements DataSet<CartesianPoint>,
        Iterator<CartesianPoint> {

    protected int dimension;
    private Random rand;
    private boolean gaussian;

    /**
     * @param dimension
     *            The number of dimensions the generated points will contain
     *
     * @param gaussian
     *            Whether the distribution is to be Gaussian or flat for each
     *            dimension
     */
    public CartesianPointGenerator(int dimension, boolean gaussian) {
        this.dimension = dimension;
        this.rand = new Random(0);
        this.gaussian = gaussian;
    }

    /**
     * @param dimension
     * @param repeatable
     *            if false, a pseudo-randomly seeded generator is used; if not,
     *            the generator is seeded with zero
     * @param gaussian
     */
    public CartesianPointGenerator(int dimension, boolean repeatable,
                                   boolean gaussian) {
        this.dimension = dimension;
        if (repeatable) {
            this.rand = new Random(0);
        } else {
            this.rand = new Random(System.currentTimeMillis());
        }
        this.gaussian = gaussian;
    }

    @Override
    public CartesianPoint randomValue() {
        return next();
    }

    public CartesianPoint next() {

        double[] point = new double[dimension];
        for (int i = 0; i < dimension; i++) {
            if (gaussian) {
                /*
                 * this is a norm dist value with mean zero and sd 1.0, so
                 * convert it to mean 0.5 and sd 0.2
                 */
                double val = rand.nextGaussian();
                // compress it so most value are in the range
                val = val / 5;
                val += 0.5;
                if (val < 0) {
                    val = 0;
                } else if (val > 1) {
                    val = 1;
                }
                point[i] = val;
            } else {
                point[i] = rand.nextDouble();
            }
        }

        return new CartesianPoint(point);
    }

    @Override
    public boolean isFinite() {
        return false;
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public void remove() {
        System.out.println("remove not implemented in " + this.getClass());
    }

    @Override
    public Iterator<CartesianPoint> iterator() {
        return this;
    }

    @Override
    public String getDataSetName() {
        String res = "Cartesian " + dimension + " dimensions";
        if (gaussian) {
            res += " (Gaussian)";
        } else {
            res += " (Non-Gaussian)";
        }
        return res;
    }

    @Override
    public int size() {
        return -1;
    }

    @Override
    public String getDataSetShortName() {
        // TODO Auto-generated method stub
        return "pointGen";
    }

}
