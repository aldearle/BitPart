package uk.al_richard.bitpart.referenceImplementation;

import org.apache.commons.math3.stat.regression.SimpleRegression;
import uk.richardconnor.metricSpaceFramework.ObjectWithDistance;
import uk.richardconnor.metricSpaceFramework.Quicksort;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Richard
 * 
 *         so the intent is to store info about a pair of reference points and
 *         the partitions they can define but.. not so simple as the main point
 *         of this is to store distance computations at both build and query
 *         time
 * @param <T> the type of the values
 */
public class SheetExclusion4p<T> extends SheetExclusion<T> {

	protected double interPivotDistance;
	protected double offset;

	private final RefPointSet<T> pointSet;
	private final int ref1;
	private final int ref2;
	protected double rotationAngle;
	protected boolean rotationEnabled;
	protected double x_offset_for_rotation;

	public SheetExclusion4p(RefPointSet<T> pointSet, int ref1, int ref2) {
		this.pointSet = pointSet;
		this.ref1 = ref1;
		this.ref2 = ref2;
		this.setInterPivotDistance(pointSet.intDist(ref1, ref2));

		this.offset = 0;
		this.setRotationEnabled(false);
	}

	public SheetExclusion4p(RefPointSet<T> pointSet, int ref1, int ref2, double offset ) {

		this( pointSet,ref1,ref2 );
		setOffset( offset );
	}


	public void setOffset( double offset ) {
		this.offset = offset;
	}

	/**
	 * inputs a list of (x,y) coordinates and calculates the gradient of the
	 * best-fit straight line, from this sets x_offset and rotationAngle
	 *
	 */
	protected void calculateTransform(List<double[]> points) {

		SimpleRegression reg = new SimpleRegression(true);

		for (double[] p : points) {
			reg.addData(p[0], p[1]);
		}
		double grad = reg.getSlope();
		double y_int = reg.getIntercept();

		if (grad != 0) {
			this.x_offset_for_rotation = -y_int / grad;
			this.rotationAngle = Math.atan(grad);
		}

	}

	protected List<double[]> getCoordinateSet(List<T> values) {

		List<double[]> res = new ArrayList<>();

		for (int i = 0; i < values.size(); i++) {
			double d1 = this.getPointSet().extDist(values.get(i), this.getRef1());
			double d2 = this.getPointSet().extDist(values.get(i), this.getRef2());

			res.add(getTwoDPoint(d1, d2));
		}
		return res;
	}

	protected double getNewX(double x, double y) {
		double newX = x - this.x_offset_for_rotation;

		if (!Double.isNaN(this.rotationAngle)) {
			newX = Math.cos(this.rotationAngle) * newX + Math.sin(this.rotationAngle) * y;
		}
		if (Double.isNaN(newX)) {
			throw new RuntimeException("newX is a nan!");
		}
		return newX;
	}

	private double[] getTwoDPoint(double d1, double d2) {
		final double x_offset = (d1 * d1 - d2 * d2) / (2 * this.interPivotDistance) + this.interPivotDistance / 2;
		final double y_offset = d1 * d1 - x_offset * x_offset;
		double[] pt = { x_offset, y_offset };
		return pt;
	}

	private double getXOffset(double[] dists) {
		double d1 = dists[this.getRef1()];
		double d2 = dists[this.getRef2()];
		double[] pt = getTwoDPoint(d1, d2);

		double x_offset = getNewX(pt[0], pt[1]);
		return x_offset;
	}

	/**
	 * @return true if the point is strictly to the left of a defined partition
	 */
	@Override
	public boolean isIn(double[] dists) {
		double x_offset = getXOffset(dists);
		return x_offset < this.offset;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see ExclusionZone#mustBeIn(java.lang.Object, double[], double)
	 * 
	 * return true if the query is a long way to the left of the defined
	 * partition...
	 */
	@Override
	public boolean mustBeIn(double[] dists, double t) {
		double x_offset = getXOffset(dists);
		return x_offset < this.offset - t;

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see ExclusionZone #mustBeIn(java.lang.Object, double[], double)
	 * 
	 * return true if the query is a long way to the right of the defined
	 * partition...
	 */
	@Override
	public boolean mustBeOut(double[] dists, double t) {
		double x_offset = getXOffset(dists);
		return x_offset >= this.offset + t;

	}

	public void setRotationEnabled(boolean rotationEnabled) {
		this.rotationEnabled = rotationEnabled;
	}

	public void setWitnesses(List<T> witnesses) {
		List<double[]> twoDpoints = getCoordinateSet(witnesses);

		if (this.rotationEnabled) {
			this.calculateTransform(twoDpoints);
		}
		/*
		 * now rotationAngle and x_offset are set, we need to get rotated xs
		 */
		@SuppressWarnings("unchecked")
		ObjectWithDistance<Object>[] owds = new ObjectWithDistance[witnesses.size()];
		for (int i = 0; i < owds.length; i++) {
			double[] pt = twoDpoints.get(i);
			double xOffset = this.rotationEnabled ? getNewX(pt[0], pt[1]) : pt[0];
			owds[i] = new ObjectWithDistance<>(null, xOffset);
		}
		Quicksort.placeMedian(owds);
		// offset is now the median x value for the rotated pointset
		this.offset = owds[owds.length / 2].getDistance();

	}

	@Override
	public RefPointSet<T> getPointSet() {
		return pointSet;
	}

	@Override
	public int getRef1() {
		return ref1;
	}

	@Override
	public int getRef2() {
		return ref2;
	}

	public double getInterPivotDistance() {
		return interPivotDistance;
	}

	private void setInterPivotDistance(double intDist) {
		this.interPivotDistance = intDist;
	}

}
