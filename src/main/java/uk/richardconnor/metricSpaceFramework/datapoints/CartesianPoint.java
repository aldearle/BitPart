package uk.richardconnor.metricSpaceFramework.datapoints;

import uk.richardconnor.metricSpaceFramework.util.OrderedList;

import java.util.Arrays;
import java.util.List;

/**
 *
 * a class representing Cartesian points, but with many utility methods for
 * memo-ising computed data
 *
 * @author Richard Connor
 *
 */
public class CartesianPoint {

    private static final double LOG_2 = Math.log(2);
    private double complexity = -1;
    private double entropy = -1;
    private double fieldTotal = -1;
    private double[] logTerms;
    private double magnitude = -1;
    private double magnitudeSq = -1;
    private List<Integer> magOrdering;
    private double[] normalisedPoint;

    private double[] point;

    /**
     * Create a new CartesianPoint based on an array of doubles
     *
     * @param point
     *            the array of doubles
     */
    public CartesianPoint(double[] point) {
        this.point = point;
    }

    /**
     * Create a new CartesianPoint based on an array of floats
     *
     * @param point
     *            the array of floats
     */
    public CartesianPoint(float[] point) {
        double[] ds = new double[point.length];
        for (int i = 0; i < point.length; i++) {
            ds[i] = point[i];
        }
        this.point = ds;
    }

    /**
     * @return the complexity of this point; lazily evaluates the calculation
     */
    public double getComplexity() {
        if (this.complexity == -1) {
            this.complexity = Math.pow(Math.E, this.getEntropy());
        }
        return this.complexity;
    }

    /**
     * @return the complexity of this point; lazily evaluates the calculation
     */
    public double getEntropy() {
        if (this.entropy == -1) {
            double acc = 0;
            for (double d : getNormalisedPoint()) {
                if (d != 0) {
                    acc -= d * Math.log(d);
                }
            }

            this.entropy = acc;
        }

        return this.entropy;
    }

    /**
     * @return an array of log terms (-x.log(x) to log base 2) for each
     *         dimension; used in SED and Jensen-Shannon; lazily evaluates the
     *         calculation
     */
    public double[] getLog2Terms() {
        if (this.logTerms == null) {
            this.logTerms = new double[this.point.length];
            double[] norm = getNormalisedPoint();
            for (int i = 0; i < this.point.length; i++) {
                double d = norm[i];
                if (d == 0) {
                    this.logTerms[i] = 0;
                } else {
                    this.logTerms[i] = -d * (Math.log(d) / LOG_2);
                }
            }
        }
        return this.logTerms;
    }

    /**
     * @return the vector magnitude of the point; used in Cosine distance;
     *         lazily evaluates the calculation
     */
    public double getMagnitude() {
        if (this.magnitude == -1) {
            getMagnitudeSq();
            this.magnitude = Math.sqrt(this.magnitudeSq);
        }
        return this.magnitude;
    }

    /**
     * @return the square of the vector magnitude of the point; used in
     *         Tanimoto; lazily evaluates the calculation distance
     */
    public double getMagnitudeSq() {
        if (this.magnitudeSq == -1) {
            double sum = 0;
            for (double d : this.point) {
                sum += d * d;
            }
            this.magnitudeSq = sum;
        }
        return this.magnitudeSq;
    }

    /**
     * @return a normalised version of the point, ie a scalar multiple such that
     *         the dimensions add to 1; lazily evaluates the calculation
     */
    public double[] getNormalisedPoint() {
        if (this.normalisedPoint == null) {
            this.normalisedPoint = new double[this.point.length];
            double tot = getFieldTotal();
            if (tot != 0) {
                for (int i = 0; i < this.point.length; i++) {
                    this.normalisedPoint[i] = this.point[i] / tot;
                }
            } else {
                for (int i = 0; i < this.point.length; i++) {
                    this.normalisedPoint[i] = 1 / (double) this.point.length;
                }
                // throw new
                // RuntimeException("can't normalise all-zero vector");
            }
        }
        return this.normalisedPoint;
    }

    /**
     * @return the array used to create the point
     */
    public double[] getPoint() {
        return this.point;
    }

    /**
     * @return a list of integers, giving the dimensions, in order, according to
     *         the magnitude of the array values
     */
    @SuppressWarnings("boxing")
    public List<Integer> magnitudeOrdering() {
        if (this.magOrdering == null) {
            OrderedList<Integer, Double> ord = new OrderedList<>(
                    this.point.length);
            int dim = 0;
            for (double v : this.point) {
                ord.add(dim, v);
                dim++;
            }
            this.magOrdering = ord.getList();
        }
        return this.magOrdering;
    }

    /**
     * @return a String representing the point in a CSV file format
     */
    public String toCsvString() {
        StringBuffer b = new StringBuffer("point"); //$NON-NLS-1$

        for (double d : this.point) {
            b.append("," + d); //$NON-NLS-1$
        }
        return b.toString();
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        StringBuffer res = new StringBuffer("Point: ["); //$NON-NLS-1$

        for (double d : this.point) {
            res.append(d + " ,");
        }
        res.delete(res.length() - 2, res.length());
        res.append(']');

        return res.toString();
    }

    /**
     * @return the total of the array values
     */
    private double getFieldTotal() {
        if (this.fieldTotal != -1) {
            return this.fieldTotal;
        } else {
            double sum = 0;
            for (double d : this.point) {
                sum += d;
            }

            this.fieldTotal = sum;
            return sum;
        }
    }

    /**
     * returns a CartesianPoint representing a given vector of floats
     *
     * @param fs
     *            vector of floats to be converted
     * @return the point
     *
     *
     */
    public static CartesianPoint toCartesianPoint(float[] fs) {
        double[] ds = new double[fs.length];
        for (int i = 0; i < fs.length; i++) {
            ds[i] = fs[i];
        }
        return new CartesianPoint(ds);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CartesianPoint)) return false;
        CartesianPoint that = (CartesianPoint) o;
        return Arrays.equals(point, that.point);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(point);
    }
}

