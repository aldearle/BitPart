package uk.al_richard.bitpart.referenceImplementation;

import uk.richardconnor.metricSpaceFramework.coreConcepts.Metric;

import java.util.Collection;
import java.util.List;

public class RefPointSet<T> {

	public final List<T> refs;
	protected Metric<T> metric;

	public RefPointSet(List<T> refs, Metric<T> metric) {
		this.refs = refs;
		this.metric = metric;
	}

	public double intDist(int ref1, int ref2) {
		return this.metric.distance(this.refs.get(ref1), this.refs.get(ref2));
	}

	public double extDist(T d, int ref) {
		return this.metric.distance(d, this.refs.get(ref));
	}

	public double[] extDists(T d) {
		double[] res = new double[this.refs.size()];
		for (int i = 0; i < this.refs.size(); i++) {
			res[i] = this.metric.distance(d, this.refs.get(i));
		}
		return res;
	}

	public double[] extDists(T d, Collection<T> res, double threshold) {
		double[] dists = new double[this.refs.size()];
		for (int i = 0; i < this.refs.size(); i++) {
			dists[i] = this.metric.distance(d, this.refs.get(i));
			if (dists[i] <= threshold) {
				res.add(this.refs.get(i));
			}
		}
		return dists;
	}

}
