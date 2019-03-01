package utilities;


import com.google.common.collect.Lists;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Trajectory  implements Serializable{


    private int verticalID;

    public int getHorizontalID() {
        return horizontalID;
    }
    private int horizontalID;
    public long getTrajectoryID() {
        return trajectoryID;
    }
    public long trajectoryID;
    public LongArrayList timestamps = new LongArrayList();
    public long getStartingTime() {
        return startingTime;
    }
    public long getEndingTime() {
        return endingTime;
    }
    private long startingTime;
    private long endingTime;

    public LongArrayList getRoadSegments() {
        return roadSegments;
    }

    public LongArrayList roadSegments = new LongArrayList();
    public Integer timeSlice = 0;


    public void setTimeSlice(Integer timeSlice) {
        this.timeSlice = timeSlice;
    }
    public Long getStartingRS(){
        return roadSegments.get(0);
    }


    public void addRoadSegment(Long roadSegment) {
//        roadSegments.add(roadSegment.intern());
        roadSegments.add(roadSegment);
    }

    public void setStartingTime(Long startingTime) {
        this.startingTime = startingTime;
    }

    public void setEndingTime(Long endingTime) {
        this.endingTime = endingTime;
    }

    public void setHorizontalID(int horizontalID) {
        this.horizontalID = horizontalID;
    }


    public Trajectory(Trajectory t, int time_slice) {
        trajectoryID = t.trajectoryID;
        roadSegments = t.roadSegments;
        startingTime = t.startingTime;
        endingTime = t.endingTime;
        timeSlice = time_slice;

    }

    public Trajectory() {

    }


    public Trajectory(Long trajectoryID) {
        this.trajectoryID = trajectoryID;
    }


    public void addSample(Long timestamp, Long roadID) {

        timestamps.add(timestamp);
//        roadSegments.add(roadID.intern());
        roadSegments.add(roadID);

    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("\ntrajectoryID:"+trajectoryID).append(", startingTime:"+startingTime+", endingTime:"+endingTime+"\n");

        for (int i = 0; i < roadSegments.size(); i++) {
            sb.append( roadSegments.get(i) + "\n");
        }

        sb.append("----------------------------------------------------------\n");
        return sb.toString();
    }

    public List<Integer> determineTimeSlices(final List<Long> timePeriods) throws Exception {
        List<Integer> timeSlices = new ArrayList<>();
        boolean foundMax = false;
        int minIndex = -1, maxIndex = -1;
        for (int i = 0; i < timePeriods.size(); i++) {


            if (startingTime >= timePeriods.get(i)) {
                minIndex = i;
            }

            if (endingTime <= timePeriods.get(i) && !foundMax) {
                foundMax = true;
                maxIndex = i;
            }

        }

        assert (minIndex != maxIndex);
        assert (minIndex < maxIndex);
        //make sure you don't need equal here
        for (int i = minIndex; i < maxIndex; i++) {
            timeSlices.add(i);
        }

        assert (!timeSlices.isEmpty());
        return timeSlices;
    }

    public void setVerticalID(int verticalID) {
        this.verticalID = verticalID;
    }

    public int getVerticalID() {
        return verticalID;
    }
}
