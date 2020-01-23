 package utilities;

import com.google.common.collect.Iterables;
import comparators.IntegerComparator;
import comparators.LongComparator;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;

/**
 * Created by giannis on 25/06/19.
 */
public class HybridPartitioner implements Serializable {

//    private final int lengthInterval;
    private final int roadSlices;
    private int temporalSlices;
    private int lengthSlices;
    private long minTimestamp;
    private long maxTimestamp;
    private long timeInterval;

    private long minRS;
    private long maxRS;
    private long roadInterval;
//    private int maxTrajLength;
//    private int minTrajLength;

    public HybridPartitioner(JavaRDD<CSVRecord> records, int rt_upperBound,  int rs_upperBound) {
        this.temporalSlices = rt_upperBound;
//        this.lengthSlices = rl_upperBound;
        this.roadSlices = rs_upperBound;

        minTimestamp = records.map(r -> r.getTimestamp()).min(new LongComparator());
        maxTimestamp = records.map(r -> r.getTimestamp()).max(new LongComparator());
        timeInterval = (maxTimestamp - minTimestamp) / temporalSlices;

        minRS = records.map(r -> r.getRoadSegment()).min(new IntegerComparator());
        maxRS = records.map(r -> r.getRoadSegment()).max(new IntegerComparator());
        roadInterval = (maxRS - minRS) / roadSlices;

//        maxTrajLength = records.groupBy(r -> r.getTrajID()).mapValues(it -> Iterables.size(it)).values().max(new IntegerComparator());
//        minTrajLength = records.groupBy(r -> r.getTrajID()).mapValues(it -> Iterables.size(it)).values().min(new IntegerComparator());

//        lengthInterval = (int) Math.ceil((maxTrajLength - minTrajLength) / (double)lengthSlices);
    }


    public int determineTimeSlice(LongArrayList timestamps) {
        long startingTime = timestamps.getLong(0);

        return determineTimeSlice(startingTime);

    }

//    public int determineLengthSlice(int length) {
//
//        int lengthSlice = (int) Math.floor((length - minTrajLength) / lengthInterval);
//        return lengthSlice;
//
//    }

    /**
     * Returns unique slice / partition id
     *
     * @param trajectory
     * @return
     */
    public int determineSlice(final Trajectory trajectory) {

        //if we have 10 slices, determineTimeSlice returns an integer [0,9]
        int timeSlice = determineTimeSlice(trajectory.getTimestamps());
        int roadSlice = determineRoadSlice(trajectory.getStartingRS());

        return timeSlice +  roadSlice*temporalSlices;
//        return roadSlice +  timeSlice*temporalSlices;

    }

    private int determineRoadSlice(Integer startingRS) {
        int roadSlice = (int) Math.floor((startingRS - minRS) / roadInterval);
        return roadSlice;
    }
    private int determineTimeSlice(long startingTime) {

        return (int) Math.floor((startingTime - minTimestamp) / timeInterval);
    }
}
