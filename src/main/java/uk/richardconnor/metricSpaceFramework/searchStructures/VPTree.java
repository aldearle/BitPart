package uk.richardconnor.metricSpaceFramework.searchStructures;

import uk.richardconnor.metricSpaceFramework.coreConcepts.Metric;
import uk.richardconnor.metricSpaceFramework.datapoints.CartesianPoint;
import uk.richardconnor.metricSpaceFramework.util.OrderedListAlt;
import uk.richardconnor.metricSpaceFramework.util.Range;

import java.util.ArrayList;
import java.util.List;

public class VPTree<T> extends SearchIndex<T> {

    private class VPTreeNode {
        private int pivot;
        private double pivotDist;
        VPTreeNode left;
        VPTreeNode right;

        VPTreeNode(int start, int end) {
            this.pivot = VPTree.this.ids[start];
            VPTree.this.dists[start] = -1;

            if (end > start) {
                // if not, then end == start which means that the pivot now acts
                // as a single leaf data node
                T piv = VPTree.this.data.get(VPTree.this.ids[start]);

                for (int i = start + 1; i <= end; i++) {
                    assert VPTree.this.metric != null;
                    final double distance = VPTree.this.metric.distance(piv,
                            VPTree.this.data.get(VPTree.this.ids[i]));
                    assert (Double.isFinite(distance)) : show(piv,
                            VPTree.this.data.get(VPTree.this.ids[i]));
                    VPTree.this.dists[i] = distance;
                }

                final int medianPos = start + ((end - start) + 1) / 2;
                quickFindMedian(start + 1, end, medianPos);

                this.pivotDist = VPTree.this.dists[medianPos];

                if (start + 1 <= medianPos) {
                    this.left = new VPTreeNode(start + 1, medianPos);
                }
                if (end >= medianPos + 1) {
                    this.right = new VPTreeNode(medianPos + 1, end);
                }
            }
        }

        private String show(T t1, T t2) {
            double[] p1 = ((CartesianPoint) t2).getPoint();
            double[] p2 = ((CartesianPoint) t2).getPoint();
            StringBuffer sb = new StringBuffer();
            for (int i : Range.range(0, p1.length)) {
                sb.append(p1[i] + ":" + p2[i] + "\n");
            }
            return sb.toString();
        }

        @SuppressWarnings("synthetic-access")
        public void nnquery() {

            final T pivotValue = VPTree.this.data.get(this.pivot);
            double qTOpDistance = VPTree.this.metric.distance(
                    VPTree.this.nnQuery, pivotValue);

            if (qTOpDistance < VPTree.this.nnThreshold) {
                nnCurrentIndex = this.pivot;
                VPTree.this.nnThreshold = qTOpDistance;
            }

            if (qTOpDistance <= this.pivotDist - VPTree.this.nnThreshold) {
                if (this.left != null) {
                    this.left.nnquery();
                }
            } else if (qTOpDistance > this.pivotDist + VPTree.this.nnThreshold) {
                if (this.right != null) {
                    this.right.nnquery();
                }
            } else {
                if (this.left != null) {
                    this.left.nnquery();
                }
                if (this.right != null) {
                    // && queryToPivotDistance1 > this.pivotDist
                    // + BalancedVPTree.this.nnThreshold) {
                    this.right.nnquery();
                }
            }
        }

        @SuppressWarnings("synthetic-access")
        public void nnquery(OrderedListAlt<Integer, Double> ol) {

            final T pivotValue = VPTree.this.data.get(this.pivot);
            double qTOpDistance = VPTree.this.metric.distance(
                    VPTree.this.nnQuery, pivotValue);

            if (qTOpDistance < VPTree.this.nnThreshold) {
                ol.add(this.pivot, qTOpDistance);
                Double t = ol.getThreshold();
                if (t != null) {
                    VPTree.this.nnThreshold = t;
                }

                // System.out.println("changing current nn to point " +
                // this.pivot
                // + " at distance " + qTOpDistance);
            }

            if (qTOpDistance <= this.pivotDist - VPTree.this.nnThreshold) {
                if (this.left != null) {
                    this.left.nnquery(ol);
                }
            } else if (qTOpDistance > this.pivotDist + VPTree.this.nnThreshold) {
                if (this.right != null) {
                    this.right.nnquery(ol);
                }
            } else {
                if (this.left != null) {
                    this.left.nnquery(ol);
                }
                if (this.right != null) {
                    // && queryToPivotDistance1 > this.pivotDist
                    // + BalancedVPTree.this.nnThreshold) {
                    this.right.nnquery(ol);
                }
            }
        }

        public void queryRef(T query, double threshold, List<Integer> results) {

            final T pivotValue = VPTree.this.data.get(this.pivot);

            double queryToPivotDistance1 = VPTree.this.metric.distance(query,
                    pivotValue);

            if (queryToPivotDistance1 < threshold) {
                results.add(this.pivot);
            }

            if (queryToPivotDistance1 <= this.pivotDist - threshold) {
                if (this.left != null) {
                    this.left.queryRef(query, threshold, results);
                }
            } else if (queryToPivotDistance1 > this.pivotDist + threshold) {
                if (this.right != null) {
                    this.right.queryRef(query, threshold, results);
                }
            } else {
                if (this.left != null) {
                    this.left.queryRef(query, threshold, results);
                }
                if (this.right != null) {
                    this.right.queryRef(query, threshold, results);
                }
            }
        }

        private void query(T query, double threshold, List<T> results) {

            final T pivotValue = VPTree.this.data.get(this.pivot);

            double queryToPivotDistance1 = VPTree.this.metric.distance(query,
                    pivotValue);

            if (queryToPivotDistance1 < threshold) {
                results.add(pivotValue);
            }

            if (queryToPivotDistance1 <= this.pivotDist - threshold) {
                if (this.left != null) {
                    this.left.query(query, threshold, results);
                }
            } else if (queryToPivotDistance1 > this.pivotDist + threshold) {
                if (this.right != null) {
                    this.right.query(query, threshold, results);
                }
            } else {
                if (this.left != null) {
                    this.left.query(query, threshold, results);
                }
                if (this.right != null) {
                    this.right.query(query, threshold, results);
                }
            }

        }

    }

    private VPTreeNode index;
    private double nnThreshold;
    private int nnCurrentIndex;

    private T nnQuery;

    private List<T> data;

    private int[] ids;

    private double[] dists;

    public VPTree(List<T> data, Metric<T> metric) {
        super(data, metric);

        this.data = data;
        this.ids = new int[data.size()];
        for (int i = 0; i < this.ids.length; i++) {
            this.ids[i] = i;
        }
        this.dists = new double[this.ids.length];
        /*
         * recursively, constructs the entire index
         */
        this.index = new VPTreeNode(0, data.size() - 1);
    }

    public int nearestNeighbour(T query) {
        this.nnThreshold = Double.MAX_VALUE;
        this.nnCurrentIndex = -1;
        this.nnQuery = query;
        this.index.nnquery();
        return this.nnCurrentIndex;
    }

    public List<Integer> nearestNeighbour(T query, int numberOfResults) {
        this.nnThreshold = Double.MAX_VALUE;
        OrderedListAlt<Integer, Double> ol = new OrderedListAlt<Integer, Double>(
                numberOfResults);
        this.nnQuery = query;
        this.index.nnquery(ol);
        return ol.getList();
    }

    public List<Integer> thresholdQueryByReference(T query, double threshold) {
        List<Integer> res = new ArrayList<Integer>();
        this.index.queryRef(query, threshold, res);
        return res;
    }

    @Override
    public List<T> thresholdSearch(T query, double threshold) {
        List<T> res = new ArrayList<T>();
        this.index.query(query, threshold, res);
        return res;
    }

    private void swap(int x, int y) {
        if (x != y) {
            double tempD = this.dists[x];
            int tempI = this.ids[x];
            this.dists[x] = this.dists[y];
            this.ids[x] = this.ids[y];
            this.dists[y] = tempD;
            this.ids[y] = tempI;
        }
    }

    /**
     * sorts just enough of the ids and dists vectors so that the entry at
     * medianPos is in the correct sorted place; partitions not relevant to that
     * are not sorted
     *
     * this is of course more general than the median but that's the only use so
     * far
     *
     * @param from
     * @param to
     * @param medianPos
     */
    protected void quickFindMedian(int from, int to, int medianPos) {

        double pivot = this.dists[to];

        int upTo = from;
        int pivotPos = to;
        /*
         * now, run into the middle of the vector from both ends, until upTo and
         * pivotPos meet
         */
        while (pivotPos != upTo) {
            if (this.dists[upTo] > pivot) {
                swap(upTo, pivotPos - 1);
                swap(pivotPos - 1, pivotPos--);
            } else {
                upTo++;
            }
        }
        /*
         * this code only places the median value correctly
         */
        if (pivotPos > medianPos && pivotPos > from) {
            quickFindMedian(from, pivotPos - 1, medianPos);
        } else if (pivotPos < medianPos && pivotPos < to) {
            quickFindMedian(pivotPos + 1, to, medianPos);
        }
    }

    /**
     * sorts the ids and dists vectors into the order of dists
     *
     * @param from
     * @param to
     * @param medianPos
     */
    protected void quickSort(int from, int to) {

        double pivot = this.dists[to];

        int upTo = from;
        int pivotPos = to;
        /*
         * now, run into the middle of the vector from both ends, until upTo and
         * pivotPos meet
         */
        while (pivotPos != upTo) {
            if (this.dists[upTo] > pivot) {
                swap(upTo, pivotPos - 1);
                swap(pivotPos - 1, pivotPos--);

            } else {
                upTo++;
            }
        }

        /*
         * here is the quicksort code if we want more than the median value
         */
        if (pivotPos > from) {
            quickSort(from, pivotPos - 1);
        }
        if (pivotPos < to) {
            quickSort(pivotPos + 1, to);
        }
    }

    @Override
    public String getShortName() {
        return "vpt";
    }

}

