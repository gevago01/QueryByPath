package trie;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import partitioners.StartingRSPartitioner;
import utilities.PartitioningMethods;
import utilities.Trajectory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Query implements Serializable {

    private int queryID;
    private static int counter;
    private long startingTime, endingTime;
    public int partitionID;
    public IntArrayList pathSegments = new IntArrayList();



    /**
     * called for time slicing
     * @param t
     * @param timestamps
     */
    public Query(Trajectory t, List<Long> timestamps) {

        this(t.getStartingTime(),t.getEndingTime(),t.roadSegments);
            List<Integer> timeSlices=determineTimeSliceLongs(timestamps);
//            partitionID=timeSlices.get(new Random().nextInt(timeSlices.size())) % Parallelism.PARALLELISM;
            partitionID =timeSlices.stream().findAny().get() ;
    }

    public Query(long startingTime, long endingTime, List<Integer> roadSegments) {
        this.startingTime=startingTime;
        this.endingTime=endingTime;
        this.pathSegments=new IntArrayList(roadSegments);

    }

    public Query(Trajectory t) {

        this(t.getStartingTime(),t.getEndingTime(),t.roadSegments);
    }

    public int getQueryID() {
        return queryID;
    }
    public void setQueryID(int queryID) {
        this.queryID = queryID;
    }

    public int getPartitionID() {
        return partitionID;
    }

    public void setPartitionID(int partition) {
        partitionID=partition;
    }


    public long getStartingTime() {
        return startingTime;
    }

    public long getEndingTime() {
        return endingTime;
    }

    public List<Integer> getPathSegments() {
        return pathSegments;
    }


    public void setPathSegments(List<Integer> pathSegments) {
//        this.pathSegments =  new LongArrayList(pathSegments);
        this.pathSegments = new IntArrayList(pathSegments);

    }

    public int getStartingRoadSegment() {
        return pathSegments.getInt(0);
    }

    public void setStartingTime(long startingTime) {
        this.startingTime = startingTime;
    }

    public void setEndingTime(long endingTime) {
        this.endingTime = endingTime;
    }

    @Override
    public String toString() {

        return "QueryPrint{" +
                ", startingTime=" + startingTime +
                ", endingTime=" + endingTime +
                ", pathSegments=" + pathSegments + '}';
    }




    public List<Integer> determineTimeSliceLongs(List<Long> timePeriods) {
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

        if (minIndex==maxIndex){
            timeSlices.add(minIndex);
        }


        //make sure you don't need equal here
        for (int i = minIndex; i < maxIndex; i++) {
            timeSlices.add(i);
        }

        return timeSlices;

    }


}
