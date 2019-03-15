package trie;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import partitioners.StartingRSPartitioner;
import utilities.PartitioningMethods;
import utilities.Trajectory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Query implements Serializable {

    private int queryID;
    private long startingTime, endingTime;
    public int timeSlice;
    private int horizontalPartition = -1;
    public IntArrayList pathSegments = new IntArrayList();
//    private int verticalID;

    public Query(Trajectory t, List<Integer> roadIntervals, PartitioningMethods pm) {

        this(t.getStartingTime(),t.getEndingTime(),t.roadSegments);
        if (pm == PartitioningMethods.VERTICAL) {
//            int x = new StartingRSPartitioner(roadIntervals,0).getPartition(t.roadSegments.get(0));
//            verticalID = x;
        }
        else if (pm == PartitioningMethods.TIME_SLICING) {
            List<Integer> timeSlices=determineTimeSlice(roadIntervals);
//            timeSlice=timeSlices.get(new Random().nextInt(timeSlices.size())) % Parallelism.PARALLELISM;
            timeSlice=timeSlices.get(new Random().nextInt(timeSlices.size())) ;
        }
        else if(pm == PartitioningMethods.HORIZONTAL) {
            //do nothing
        }
    }

    /**
     * called for time slicing
     * @param t
     * @param timestamps
     */
    public Query(Trajectory t, List<Long> timestamps) {

        this(t.getStartingTime(),t.getEndingTime(),t.roadSegments);
            List<Integer> timeSlices=determineTimeSliceLongs(timestamps);
//            timeSlice=timeSlices.get(new Random().nextInt(timeSlices.size())) % Parallelism.PARALLELISM;
            timeSlice=timeSlices.get(new Random().nextInt(timeSlices.size())) ;
    }

    public Query(long startingTime, long endingTime, List<Integer> roadSegments) {
        this.startingTime=startingTime;
        this.endingTime=endingTime;
//        this.pathSegments=roadSegments;
        this.pathSegments=new IntArrayList(roadSegments);
    }

    public int getQueryID() {
        return queryID;
    }
    public void setQueryID(int queryID) {
        this.queryID = queryID;
    }

    public int getTimeSlice() {
        return timeSlice;
    }

    public int getHorizontalPartition() {
        return horizontalPartition;
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


    public List<Integer> determineTimeSlice(List<Integer> timePeriods) {
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

        //make sure you don't need equal here
        for (int i = minIndex; i < maxIndex; i++) {
            timeSlices.add(i);
        }

        return timeSlices;

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

        //make sure you don't need equal here
        for (int i = minIndex; i < maxIndex; i++) {
            timeSlices.add(i);
        }

        return timeSlices;

    }

    public void setHorizontalPartition(int horizontalPartition) {
        this.horizontalPartition = horizontalPartition;
    }

//    public void setVerticalID(int verticalID) {
//        this.verticalID = verticalID;
//    }
//    public long getVerticalID() {
//        return verticalID;
//    }
}
