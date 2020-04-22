package uk.richardconnor.metricSpaceFramework.testloads;

import uk.richardconnor.metricSpaceFramework.coreConcepts.Metric;
import uk.richardconnor.metricSpaceFramework.datapoints.CartesianPoint;
import uk.richardconnor.metricSpaceFramework.datapoints.cartesian.Euclidean;

import java.util.List;

/**
 * idea is to create, with as little syntactic fuss as possible, various test
 * loads useful for measuring metric space indexing techniques
 *
 * @author Richard Connor
 */
public class TestContext {

    /**
     * different types of data; SISAP colors and nasa files, various dimensions
     * of generated Euclidean spaces
     *
     * @author Richard Connor
     */
    public enum Context {
        colors, nasa, euc10, euc20, euc30, euc100, euc1000, euc10000
    }

    ;

    private Context context;

    /**
     * @return the context
     */
    public Context getContext() {
        return this.context;
    }

    private TestLoad tl;
    private List<CartesianPoint> queries;
    private List<CartesianPoint> refPoints;
    private List<CartesianPoint> data;
    private Metric<CartesianPoint> metric;
    private int dataSize;
    private double threshold;

    /**
     * standard 10 dimensional test context, most commonly used!
     *
     * @param size the size of the dataset created
     * @throws Exception
     */
    public TestContext(int size) throws Exception {
        setParams(Context.euc10);
        initialise(size);
    }

    public TestContext(Context c, int datasize) throws Exception {
        setParams(c);
        initialise(datasize);
    }

    public TestContext(Context c) throws Exception {
        setParams(c);
        initialise(1000 * 1000);
    }

    protected void initialise(int datasize) throws Exception {
        switch (this.context) {
            case euc10: {
                this.tl = new TestLoad(10, datasize, true, true);
                this.setThreshold(CartesianThresholds.getThreshold("euc", 10, 1));
            }
            ;
            break;
            case euc20: {
                this.tl = new TestLoad(20, datasize, true, true);
                this.setThreshold(CartesianThresholds.getThreshold("euc", 20, 1));
            }
            ;
            break;
            case euc30: {
                this.tl = new TestLoad(30, datasize, true, true);
                this.setThreshold(CartesianThresholds.getThreshold("euc", 30, 1));
            }
            ;
            break;
            case euc100: {
                this.tl = new TestLoad(100, datasize, true, true);
                this.setThreshold(CartesianThresholds.getThreshold("euc", 100, 1));
            }
            ;
            break;
            case euc1000: {
                this.tl = new TestLoad(1000, datasize, true, true);
                this.setThreshold(CartesianThresholds.getThreshold("euc", 1000, 1));
            }
            ;
            break;
            case euc10000: {
                this.tl = new TestLoad(10000, datasize, true, true);
                this.setThreshold(CartesianThresholds.getThreshold("euc", 10000, 1));
            }
            ;
            break;
            case nasa: {
                this.tl = new TestLoad(TestLoad.SisapFile.nasa);
                this.setThreshold(TestLoad
                        .getSisapThresholds(TestLoad.SisapFile.nasa)[0]);
            }
            ;
            break;
            case colors: {
                this.tl = new TestLoad(TestLoad.SisapFile.colors);
                this.setThreshold(TestLoad
                        .getSisapThresholds(TestLoad.SisapFile.colors)[0]);
            }
            ;
            break;
            default: {
                throw new Exception("unexpected test data specified");
            }
        }
    }

    protected void setParams(Context c) {
        this.dataSize = -1;
        this.context = c;
        this.metric = new Euclidean<>();
    }

    public List<CartesianPoint> getData() {
        return this.tl.getDataCopy();
    }

    public List<CartesianPoint> getDataCopy() {
        return this.tl.getDataCopy();
    }

    public List<CartesianPoint> getRefPoints() {
        return this.refPoints;
    }

    public List<CartesianPoint> getQueries() {
        return this.queries;
    }

    public void setSizes(int queries, int refPoints) {
        this.queries = this.tl.getQueries(queries);
        this.refPoints = this.tl.getQueries(refPoints);
    }

    public double getThreshold() {
        return threshold;
    }

    public double[] getThresholds() {
        switch (this.context) {
            case colors: {
                return TestLoad.getSisapThresholds(TestLoad.SisapFile.colors);
            }
            case nasa: {
                return TestLoad.getSisapThresholds(TestLoad.SisapFile.nasa);
            }
            case euc10: {
                double[] t = new double[3];
                t[0] = CartesianThresholds.getThreshold("euc", 10, 1);
                t[1] = CartesianThresholds.getThreshold("euc", 10, 2);
                t[2] = CartesianThresholds.getThreshold("euc", 10, 4);
                return t;
            }
            case euc20: {
                double[] t = new double[3];
                t[0] = CartesianThresholds.getThreshold("euc", 20, 1);
                t[1] = CartesianThresholds.getThreshold("euc", 20, 2);
                t[2] = CartesianThresholds.getThreshold("euc", 20, 4);
                return t;
            }
            default: {
                throw new RuntimeException("not implemented in TestContext");
            }
        }
    }

    public void setThreshold(double threshold) {
        this.threshold = threshold;
    }

    public Metric<CartesianPoint> metric() {
        return this.metric;
    }

    public int dataSize() {
        return this.tl.dataSize();
    }

}
