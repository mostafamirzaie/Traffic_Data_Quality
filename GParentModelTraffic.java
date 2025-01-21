package org.apache.flink;



import java.util.Date;

public class GParentModelTraffic {

    public double intercept;
    public double preCount;
    public double fCount;

    public GParentModelTraffic(double intercept, double preCount, double fCount)
    {
        this.intercept = intercept;
        this.fCount = preCount;
        this.fCount = fCount;
    }

    public String toString()
    {
        return    " |" + this.intercept
                + " |" + this.preCount
                + " |" + this.fCount + " > ";
    }

}
