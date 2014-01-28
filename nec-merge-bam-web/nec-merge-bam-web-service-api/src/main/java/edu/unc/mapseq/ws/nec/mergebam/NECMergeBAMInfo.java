package edu.unc.mapseq.ws.nec.mergebam;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "NECMergeBAMInfo", propOrder = {})
@XmlRootElement(name = "NECMergeBAMInfo")
public class NECMergeBAMInfo {

    private Integer passedReads;

    private Float aligned;

    private Float paired;

    private String bestMatch;

    public NECMergeBAMInfo() {
        super();
    }

    public Integer getPassedReads() {
        return passedReads;
    }

    public void setPassedReads(Integer passedReads) {
        this.passedReads = passedReads;
    }

    public Float getAligned() {
        return aligned;
    }

    public void setAligned(Float aligned) {
        this.aligned = aligned;
    }

    public Float getPaired() {
        return paired;
    }

    public void setPaired(Float paired) {
        this.paired = paired;
    }

    public String getBestMatch() {
        return bestMatch;
    }

    public void setBestMatch(String bestMatch) {
        this.bestMatch = bestMatch;
    }

    @Override
    public String toString() {
        return String.format("NECMergeBAMInfo [passedReads=%s, aligned=%s, paired=%s, bestMatch=%s]", passedReads,
                aligned, paired, bestMatch);
    }

}
