package edu.nwmissouri.springbeam.adhikari;

import java.io.Serializable;
import java.util.ArrayList;

public class RankedPage implements Serializable {
    String name = "unknown.md";
    Double rank = 1.000;
    ArrayList<VotingPage> voters = new ArrayList<VotingPage>();

    /**
     * 
     * @param nameIn
     * @param votersIn
     */
    public RankedPage(String nameIn, ArrayList<VotingPage> votersIn) {
        
        this.name = nameIn;
        this.voters = votersIn;
    }
    /**
     * 
     * @param nameIn
     * @param rankIn
     * @param votersIn
     */
    public RankedPage(String nameIn, Double rankIn, ArrayList<VotingPage> votersIn){
        this.name = nameIn;
        this.rank= rankIn;
        this.voters= votersIn;
    }

    public Double getRank(){
        return this.rank;
    }

    public ArrayList<VotingPage> getVoters(){
        return this.voters;
    }
    
    @Override
    public String toString(){
        return String.format("%s, %.5s, %s", this.name, this.rank, this.voters.toString());
    }
}
