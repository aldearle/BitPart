package uk.richardconnor.metricSpaceFramework;

public class ObjectWithDistance<T> implements Comparable<ObjectWithDistance<T>> {

    private double distance;
    private T pointVal;

    /**
     * @return the pointVal
     */
    public T getValue() {
        return this.pointVal;
    }

    /**
     * @return the distance
     */
    public double getDistance() {
        return this.distance;
    }

    /**
     * @param d new distance
     */
    public void setDistance(double d) {
        this.distance = d;
    }

    public ObjectWithDistance(T point, double distance) {
        this.pointVal = point;
        this.distance = distance;
    }

    @Override
    public int compareTo(ObjectWithDistance<T> arg0) {
        if (this.distance < arg0.distance) {
            return -1;
        } else if (this.distance > arg0.distance) {
            return 1;
        } else
            return 0;
    }

    @Override
    public String toString() {
        return this.pointVal.toString() + " (" + this.distance + ")";
    }

}

