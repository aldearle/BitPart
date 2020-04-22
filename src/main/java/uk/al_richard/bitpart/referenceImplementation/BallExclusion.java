package uk.al_richard.bitpart.referenceImplementation;


import uk.richardconnor.metricSpaceFramework.ObjectWithDistance;
import uk.richardconnor.metricSpaceFramework.Quicksort;

import java.util.List;
import java.util.Objects;

/**
 * @author newrichard
 * 
 * 
 * @param <T>
 *            the type of the values
 */
public class BallExclusion<T> extends ExclusionZone<T> {

	private final RefPointSet<T> pointSet;
	private int ref;
	private double radius;
    private double min_witness = Double.MAX_VALUE; // easily checked to be wrong if unset
    private double max_witness = Double.MAX_VALUE; // easily checked to be wrong if unset
    private double lower_median_quartile = Double.MAX_VALUE; // easily checked to be wrong if unset
    private double upper_median_quartile = Double.MAX_VALUE; // easily checked to be wrong if unset


    public BallExclusion(RefPointSet<T> pointSet, int ref, double radius) {
		this.pointSet = pointSet;
		this.setRef(ref);
		this.radius = radius;
	}

    public void setWitnesses(List<T> witnesses) {
        ObjectWithDistance<Integer>[] owds = new ObjectWithDistance[witnesses.size()];
        for (int i = 0; i < witnesses.size(); i++) {
            double d1 = getPointSet().extDist(witnesses.get(i), this.getRef());
            owds[i] = new ObjectWithDistance<>(0, d1);
        }
        Quicksort.placeMedian(owds);
        this.radius = owds[owds.length / 2].getDistance();
        //
        // LinearInterpolator li = new LinearInterpolator();
        // PolynomialSplineFunction sp = li.interpolate(null, null);
        // PolynomialFunction[] x = sp.getPolynomials();
    }

	public void setWitnessesQuartiles(List<T> witnesses) {
		ObjectWithDistance<Integer>[] owds = new ObjectWithDistance[witnesses.size()];
		for (int i = 0; i < witnesses.size(); i++) {
			double d1 = getPointSet().extDist(witnesses.get(i), this.getRef());
			owds[i] = new ObjectWithDistance<>(i, d1);   //i was zero
		}
		Quicksort.sort(owds); // placeMedian

        int quartile_position = owds.length / 4;
        int median_position = owds.length / 2;

		this.min_witness = owds[0].getDistance();
		this.max_witness = owds[owds.length-1].getDistance();
		this.radius = owds[median_position].getDistance();

		this.lower_median_quartile = owds[quartile_position].getDistance();
		this.upper_median_quartile = owds[median_position + quartile_position].getDistance();
		//
		// LinearInterpolator li = new LinearInterpolator();
		// PolynomialSplineFunction sp = li.interpolate(null, null);
		// PolynomialFunction[] x = sp.getPolynomials();
	}

    public double setWitnessesPercentage(List<T> witnesses, int percentage_position) {
        ObjectWithDistance<Integer>[] owds = new ObjectWithDistance[witnesses.size()];
        for (int i = 0; i < witnesses.size(); i++) {
            double d1 = getPointSet().extDist(witnesses.get(i), this.getRef());
            owds[i] = new ObjectWithDistance<>(i, d1);   //i was zero
        }
        Quicksort.sort(owds); // placeMedian

        this.radius = owds[ ( percentage_position * witnesses.size() ) / 100 ].getDistance();
        return this.radius;
    }

    public double getLowerQuartileWitnessDistance() {
        return lower_median_quartile;
    }

    public double getUpperQuartileWitnessDistance() {
        return upper_median_quartile;
    }

    public double getMinWitnessDistance() {
        return min_witness;
    }

    public double getMaxWitnessDistance() {
        return max_witness;
    }

	/**
	 * @return the radius
	 */
	public double getRadius() {
		return radius;
	}

	/**
	 * @param radius the radius to set
	 */
	public void setRadius(double radius) {
		this.radius = radius;
	}

	/**
	 * @return true if the point is to the left of a defined partition
	 * 
	 *         this is a bit weird because of the single-distance calculation
	 *         optimisation...
	 */
	@Override
	public boolean isIn(double[] dists) {
		return dists[this.getRef()] < this.radius;
	}

	@Override
	public boolean mustBeIn(double[] dists, double t) {
		return dists[this.getRef()] < this.radius - t;
	}

	@Override
	public boolean mustBeOut(double[] dists, double t) {
		return dists[this.getRef()] >= this.radius + t;
	}

	public Integer getIndex() {
		return getRef();
	}

	public RefPointSet<T> getPointSet() {
		return pointSet;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof BallExclusion)) return false;
		BallExclusion<?> that = (BallExclusion<?>) o;
		return getRef() == that.getRef();
	}

	@Override
	public int hashCode() {

		return Objects.hash(getRef());
	}

	public int getRef() {
		return ref;
	}

	public void setRef(int ref) {
		this.ref = ref;
	}
}
