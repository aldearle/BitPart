package uk.al_richard.bitpart.util;

public class PointDistance<T> {
    public final T point1;
    public final T point2;
    public double distance;

    public PointDistance (T point1, T point2, double distance) {
        this.point1 = point1;
        this.point2 = point2;
        this.distance = distance;

    }
}
