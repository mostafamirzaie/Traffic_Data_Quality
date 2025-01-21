package com.company;

public class Candidates {

    public int id;
    public int candidateId;
    public double distance;

    public Candidates(int id, int candidateId, double distance)
    {
        this.id = id;
        this.candidateId = candidateId;
        this.distance = distance;
    }

    public String toString()
    {
        return    "  <Id: " + this.id
                + " |" + this.candidateId
                + " | " + this.distance + "> ";
    }
}
