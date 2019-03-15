package partitioners;

import org.apache.spark.Partitioner;

import java.util.List;

/**
 * Created by giannis on 11/12/18.
 * This class is being used for Vertical Partitioning
 * for partitioning the road segment ids
 */
public class StartingRSPartitioner extends Partitioner {
    List<Integer> roadIntervals;
    private int verticalPartitionSize;

    public StartingRSPartitioner(List<Integer> roadIntervals, int nofVerticalSlices) {
        this.roadIntervals = roadIntervals;
        verticalPartitionSize = nofVerticalSlices;

    }

//    public int getPartition(Long startingRS) {
//
////        for (int i = 0; i < roadIntervals.size()-1; i++) {
////            if (startingRS >= roadIntervals.get(i) && startingRS <= roadIntervals.get(i + 1)) {
////
////                return i ;
////            }
////        }
//        int x;
//        try {
//            x = Math.toIntExact(startingRS);
//        } catch (ArithmeticException e) {
//            x = Integer.MAX_VALUE;
//        }
//        return x;
//    }

    @Override
    public int numPartitions() {
        return 1;
    }

    //    @Override
//    public int getPartition(Object startingRoadSegment) {
//
//        Long startingRS = (Long) startingRoadSegment;
//
//        for (int i = 0; i < roadIntervals.size()-1; i++) {
//            if (startingRS >= roadIntervals.get(i) && startingRS <= roadIntervals.get(i + 1)) {
//                return i % numPartitions();
//            }
//        }
//        System.err.println("This should never happen");
//        //return max partition
//        return (roadIntervals.size()-1) % numPartitions();
//    }
    @Override
    public int getPartition(Object startingRoadSegment) {

        Integer startingRS = (Integer) startingRoadSegment;
        return startingRS % numPartitions();

    }
}
