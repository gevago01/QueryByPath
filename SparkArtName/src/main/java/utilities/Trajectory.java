package utilities;


import com.google.common.collect.Lists;
import it.unimi.dsi.fastutil.ints.IntArrayList;
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
    public int getTrajectoryID() {
        return trajectoryID;
    }
    public int trajectoryID;

    public LongArrayList getTimestamps() {
        return timestamps;
    }

    public LongArrayList timestamps = new LongArrayList();
    public long getStartingTime() {
        return startingTime;
    }
    public long getEndingTime() {
        return endingTime;
    }
    private long startingTime;
    private long endingTime;

    public IntArrayList getRoadSegments() {
        return roadSegments;
    }

    public IntArrayList roadSegments = new IntArrayList();

    public Integer getTimeSlice() {
        return timeSlice;
    }

    public Integer timeSlice = 0;


    public void setTimeSlice(Integer timeSlice) {
        this.timeSlice = timeSlice;
    }
    public int getStartingRS(){
        return roadSegments.get(0);
    }


    public void addRoadSegment(int roadSegment) {
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


    public Trajectory(int trajectoryID) {
        this.trajectoryID = trajectoryID;
    }


    public void addSample(Long timestamp, int roadID) {

        timestamps.add(timestamp);
//        roadSegments.add(roadID.intern());
        roadSegments.add(roadID);

    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < roadSegments.size(); i++) {
            sb.append(trajectoryID).append(", ").append(timestamps.getLong(i)).append(", "). append( roadSegments.getInt(i) + "\n");
        }

        sb.deleteCharAt(sb.length()-1);
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

    public void setTimestampAt(int j, long randomStartTime) {

        timestamps.set(j,randomStartTime);
    }

    public void setRoadSegmentAt(int j, int roadSegment) {

        roadSegments.set(j,roadSegment);
    }
}
