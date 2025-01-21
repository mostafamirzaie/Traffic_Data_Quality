package com.company;

import java.util.Date;

public class Crash {
    public Date date;
    public int light;
    public int surface;
    public int defect;
    public double latitude;
    public double longitude;

    public Crash(Date date, int light, int surface, int defect, double latitude, double longitude)
    {
        this.date = date;
        this.light = light;
        this.surface = surface;
        this.defect = defect;
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public String toString()
    {
        return    "  < " + this.date
                + " |" + this.light
                + " |" + this.surface
                + " |" + this.defect
                + " |" + this.latitude
                + " | " + this.longitude + "> ";
    }
}
