package utilities;


import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Trajectory implements Serializable, Comparable<Trajectory> {

    public int trajectoryID;
    private int partitionID;
    public IntArrayList roadSegments = new IntArrayList();
//    ShortArrayLi
    public LongArrayList timestamps = new LongArrayList();
    private long hilbertValue;

    public LongArrayList getTimestamps() {
        return timestamps;
    }

    public int getTrajectoryID() {
        return trajectoryID;
    }

    public long getStartingTime() {
        return timestamps.getLong(0);
    }

    public long getEndingTime() {
        return timestamps.getLong(timestamps.size()-1);
    }

    public IntArrayList getRoadSegments() {
        return roadSegments;
    }



    public int getStartingRS() {
        return roadSegments.getInt(0);
    }


    public long getHilbertValue() {
        return hilbertValue;
    }

    public void setHilbertValue(long hilbertValue) {
        this.hilbertValue = hilbertValue;
    }

    public void addRoadSegment(int roadSegment) {
//        roadSegments.add(roadSegment.intern());
        roadSegments.add(roadSegment);
    }


    public Trajectory(Trajectory t, int time_slice) {
        trajectoryID = t.trajectoryID;
        roadSegments = t.roadSegments;
        partitionID = time_slice;

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
            sb.append(trajectoryID).append(", ").append(", ").append(roadSegments.getInt(i) + "\n");
        }

//        sb.deleteCharAt(sb.length() );
        return sb.toString();
    }




    public void setPartitionID(int partitionID) {
        this.partitionID = partitionID;
    }

    public int getPartitionID() {
        return partitionID;
    }

    public void setTimestampAt(int j, long randomStartTime) {

        timestamps.set(j, randomStartTime);
    }

    public void setRoadSegmentAt(int j, int roadSegment) {

        roadSegments.set(j, roadSegment);
    }

    public void trimTimestamps(int size) {
        //leave timestamps unchanged, but trim to size
        timestamps.trim(size);
    }

    public void setRoadSegments(IntArrayList roadSegments) {
        this.roadSegments = roadSegments;
    }

    public void setTimestamps(LongArrayList timestamps) {
        this.timestamps = timestamps;
    }

    public static List<Integer> determineTimeSlice(Trajectory t, List<Long> timePeriods) {
        List<Integer> timeSlices = new ArrayList<>();
        boolean foundMax = false;
        int minIndex = -1, maxIndex = -1;
        for (int i = 0; i < timePeriods.size(); i++) {


            if (t.getStartingTime() >= timePeriods.get(i)) {
                minIndex = i;
            }

            if (t.getEndingTime() <= timePeriods.get(i) && !foundMax) {
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

    @Override
    public int compareTo(Trajectory t){
        int compare = getStartingRS()-t.getStartingRS();

        //they belong to the same road segment
        if (compare==0){
            compare= ((Long) getStartingTime()).compareTo(t.getStartingTime());
        }

        return compare;

    }
}
