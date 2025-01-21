package com.company;

import java.util.Date;

public class Crime {

    public Date date;
    public String situation;
    public double latitude;
    public double longitude;

    public Crime(Date date, String situation, double latitude, double longitude)
    {
        this.date = date;
        this.situation = situation;
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public String toString()
    {
        return    "  < " + this.date
                + " |" + this.situation
                + " |" + this.latitude
                + " | " + this.longitude + "> ";
    }
}
