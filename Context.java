package com.company;

import java.util.Date;

public class Context {
    public Date date;
    public int vCount;
    public int parkEvent;
    public int crash;
    public int light;
    public int surface;
    public int defect;
    public int crime;

    public Context(Date date, int vCount, int parkEvent, int crash, int light, int surface, int defect, int crime)
    {
        this.date = date;
        this.vCount = vCount;
        this.parkEvent = parkEvent;
        this.crash = crash;
        this.light = light;
        this.surface = surface;
        this.defect = defect;
        this.crime = crime;
    }

    public String toString()
    {
        return    "  < " + this.date
                + " |" + this.vCount
                + " |" + this.parkEvent
                + " |" + this.crash
                + " |" + this.light
                + " |" + this.surface
                + " |" + this.defect
                + " | " + this.crime + " > ";
    }
}
