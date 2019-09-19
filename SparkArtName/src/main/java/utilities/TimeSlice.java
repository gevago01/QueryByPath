package utilities;

import comparators.IntegerComparator;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.spark.api.java.JavaRDD;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by giannis on 18/06/19.
 */
public class TimeSlice {
    
    private long lowerTime;

    public long getLowerTime() {
        return lowerTime;
    }

    public long getUpperTime() {
        return upperTime;
    }

    public int getTid() {
        return tid;
    }

    private long upperTime;
    private int tid;
    private int minRS;
    private int maxRS;

    private static List<Integer> rsSlices=new ArrayList<>();



    public TimeSlice(long lower, long upper, int id) {
        this.lowerTime =lower;
        this.upperTime =upper;
        this.tid = id;
    }

    public static void initializeRSIntervals(JavaRDD<CSVRecord> records, int nofRoadSlices) {
        int minRS= records.map(r -> r.getRoadSegment()).min(new IntegerComparator());
        int maxRS = records.map(r -> r.getRoadSegment()).max(new IntegerComparator());

        int roadInterval  =  (maxRS-minRS) / nofRoadSlices;

        for (int i = minRS; i <maxRS ; i+=roadInterval) {

            rsSlices.add(i);

        }
    }

    public static int determineRoadSlices(List<Integer> roadSegments) {

        for (int i = 0; i < rsSlices.size() -1; i++) {
            if (roadSegments.get(0) >= rsSlices.get(i) &&  roadSegments.get(0) <= rsSlices.get(i+1)){
                return i;

            }

        }
        return 0;
    }

}
