package uk.al_richard.bitpart.referenceImplementation;

import uk.richardconnor.metricSpaceFramework.ObjectWithDistance;
import uk.richardconnor.metricSpaceFramework.Quicksort;

import java.util.List;

public class SheetExclusion3p<T> extends SheetExclusion<T> {

	private double offset;
	private RefPointSet<T> pointSet;

	private final int ref1;
	private final int ref2;
	private double interPivotDistance;

	public SheetExclusion3p(RefPointSet<T> pointSet, int ref1, int ref2) {
		this.setPointSet(pointSet);
		this.ref1 = ref1;
		this.ref2 = ref2;
		this.setInterPivotDistance(pointSet.intDist(ref1, ref2));
		this.setOffset(0);
	}

	public SheetExclusion3p(RefPointSet<T> pointSet, int ref1, int ref2, double sheet_offset) {
		this( pointSet,ref1,ref2 );
		setOffset(getOffset());
	}

	public void setOffset( double offset ) {
		this.offset = offset;
	}

	/**
	 * @return true if the point is strictly to the left of a defined partition
	 * 
	 *         this is a bit weird because of the single-distance calculation
	 *         optimisation...
	 */
	@Override
	public boolean isIn(double[] dists) {
		/*
		 * a positive offset means that most points are to the right of centre, ie d1 >
		 * d2 on the whole
		 */
		return (dists[this.getRef1()] - dists[this.getRef2()]) - this.getOffset() < 0;
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
		return (dists[this.getRef1()] - dists[this.getRef2()]) - this.getOffset() < -(2 * t);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see ExclusionZone#mustBeIn(java.lang.Object, double[], double)
	 * 
	 * return true if the query is a long way to the right of the defined
	 * partition...
	 */
	@Override
	public boolean mustBeOut(double[] dists, double t) {
		return (dists[this.getRef1()] - dists[this.getRef2()]) - this.getOffset() >= (2 * t);
	}

	public void setWitnesses(List<T> witnesses) {
		@SuppressWarnings({ "unchecked" })
		ObjectWithDistance<T>[] owds = new ObjectWithDistance[witnesses.size()];
		for (int i = 0; i < witnesses.size(); i++) {
			double d1 = this.getPointSet().extDist(witnesses.get(i), this.getRef1());
			double d2 = this.getPointSet().extDist(witnesses.get(i), this.getRef2());
			owds[i] = new ObjectWithDistance<>(null, d1 - d2);
		}

		Quicksort.placeMedian(owds);
		this.setOffset(owds[owds.length / 2].getDistance());
	}

	@Override
	public RefPointSet<T> getPointSet() {
		return pointSet;
	}

	public void setPointSet(RefPointSet<T> pointSet) {
		this.pointSet = pointSet;
	}

	@Override
	public int getRef1() {
		return ref1;
	}

	@Override
	public int getRef2() {
		return ref2;
	}

	public double getOffset() {
		return offset;
	}

	private void setInterPivotDistance(double intDist) {
		this.interPivotDistance = intDist;
	}

	@Override
	public double getInterPivotDistance() {
		return interPivotDistance;
	}
}
