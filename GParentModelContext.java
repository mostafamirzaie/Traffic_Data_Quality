package org.apache.flink;



import java.util.Date;

public class GParentModelContext {

    public double intercept;
    public double fCount;
    public double wType;
    public double visibility;
    public double crash;
    public double crime;
    public double parkEvent;

    public GParentModelContext(double intercept, double fCount, double wType, double visibility, double crash, double crime, double parkEvent)
    {
        this.intercept = intercept;
        this.fCount = fCount;
        this.wType = wType;
        this.visibility = visibility;
        this.crash = crash;
        this.crime = crime;
        this.parkEvent = parkEvent;
    }

    public String toString()
    {
        return    " |" + this.intercept
                + " |" + this.fCount
                + " |" + this.wType
                + " |" + this.visibility
                + " |" + this.crash
                + " | " + this.crime
                + " | " + this.parkEvent + " > ";
    }

}