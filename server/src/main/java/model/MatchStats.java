package model;

public class MatchStats {

    private Integer numLlikes;
    private Integer numDislikes;

    public MatchStats(Integer numLlikes, Integer numDislikes) {
        this.numLlikes = numLlikes;
        this.numDislikes = numDislikes;
    }

    public Integer getNumLlikes() {
        return numLlikes;
    }

    public void setNumLlikes(Integer numLlikes) {
        this.numLlikes = numLlikes;
    }

    public Integer getNumDislikes() {
        return numDislikes;
    }

    public void setNumDislikes(Integer numDislikes) {
        this.numDislikes = numDislikes;
    }
}
