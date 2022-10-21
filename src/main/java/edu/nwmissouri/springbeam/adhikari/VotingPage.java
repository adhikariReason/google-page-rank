package edu.nwmissouri.springbeam.adhikari;

import java.io.Serializable;

public class VotingPage implements Serializable{

    String name = "unknown";
    Integer votes = 0;
    Double rank = 1.0;

    /**
     * 
     * @param nameIn
     * @param contributorVotes
     */
    public VotingPage(String nameIn, Integer votesIn) {
        this.name = nameIn;
        this.votes = votesIn;
    }
    /**
     * 
     * @param nameIn
     * @param rankIn
     * @param votesIn
     */
    public VotingPage(String nameIn, Double rankIn, Integer votesIn){
        this.name = nameIn;
        this.votes = votesIn;
        this.rank = rankIn;
    }
    
    public String getNames(){
        return this.name;
    }

    public Integer getVotes(){
        return this.votes;
    }

    public Double getRank(){
        return this.rank;
    }

    @Override
    public String toString(){
        return String.format("%s, %.5f, %d", this.name, this.rank, this.votes);
    }
}
