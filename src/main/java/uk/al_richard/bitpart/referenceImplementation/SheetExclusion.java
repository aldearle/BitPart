package uk.al_richard.bitpart.referenceImplementation;

import java.util.List;

public abstract class SheetExclusion<T> extends ExclusionZone<T> {
    public abstract void setWitnesses(List<T> witnesses);

    public abstract RefPointSet<T> getPointSet();

    public abstract int getRef1();

    public abstract int getRef2();

    public abstract double getInterPivotDistance();


}
