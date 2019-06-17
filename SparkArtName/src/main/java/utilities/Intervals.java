package utilities;

import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by giannis on 29/03/19.
 */
public class Intervals {

    public static List<Long> getIntervals(int numberOfSlices, long minValue, long maxValue) {
        final Long timeInt = (maxValue - minValue) / numberOfSlices;

        List<Long> intervals = new ArrayList<>();

        long value = minValue;
        for (int i = 0; i < numberOfSlices; i++) {
            intervals.add(value);
            value += timeInt;
        }
        intervals.add(maxValue);


        return intervals;
    }

    public static HashMap<Integer, Integer> sliceTSToBuckets2(JavaPairRDD<Integer, Trajectory> trajectoryDataset, final int bucketCapacity) {


        List<Tuple2<Long, Iterable<Tuple2<Integer, Trajectory>>>> groupedTrajs = trajectoryDataset.
                groupBy(new Function<Tuple2<Integer, Trajectory>, Long>() {
                    @Override
                    public Long call(Tuple2<Integer, Trajectory> v1) throws Exception {
                        return v1._2().getStartingTime();
                    }
                }).sortByKey().collect();
        HashMap<Integer,Integer> partitioning = assignTrajsToBucketsTS(groupedTrajs,bucketCapacity);
//        HashMap<Integer,Integer> partitioning = assignTrajsToBucketsTS3(trajectoryDataset.collect(),bucketCapacity);




        return partitioning;



    }

    private static HashMap<Integer,Integer> assignTrajsToBucketsTS3(List<Tuple2<Integer, Trajectory>> collect, int bucketCapacity) {
        HashMap<Integer,Integer> partitioning = new HashMap<>();
        int sum = 0;
        int bucketNumber = 0;

        for (Tuple2<Integer, Trajectory> trajAtThisTimestamp: collect) {


                partitioning.put(trajAtThisTimestamp._2().getTrajectoryID(),bucketNumber);
                sum++;
                if (sum>=bucketCapacity){
                    ++bucketNumber;
                    sum=0;
                }

        }

        return partitioning;
    }

    private static HashMap<Integer,Integer> assignTrajsToBucketsTS(List<Tuple2<Long, Iterable<Tuple2<Integer, Trajectory>>>> groupedTrajs, int bucketCapacity) {
        HashMap<Integer,Integer> partitioning = new HashMap<>();
        int sum = 0;
        int bucketNumber = 0;
        Long currentT=Long.MIN_VALUE;

        for (Tuple2<Long, Iterable<Tuple2<Integer, Trajectory>>> trajsAtThisTimestamp: groupedTrajs) {

//            if (trajsAtThisTimestamp._1()<currentT){
//                currentT=trajsAtThisTimestamp._1();
//            }
//            else{
//                System.err.println("wrong order");
//                System.exit(1);
//            }
            for (Tuple2<Integer, Trajectory> trajectory: trajsAtThisTimestamp._2()){

                partitioning.put(trajectory._2().getTrajectoryID(),bucketNumber);
                sum++;
                if (sum>=bucketCapacity){
                    ++bucketNumber;
                    sum=0;
                }
            }

        }

        return partitioning;

    }

    private static HashMap<Integer, Integer> assignTrajsToBuckets(List<Iterable<Tuple2<Integer, Trajectory>>> groupedTrajs, int bucketCapacity) {
        HashMap<Integer,Integer> partitioning = new HashMap<>();
        int sum = 0;
        int bucketNumber = 0;
        for (Iterable<Tuple2<Integer, Trajectory>> trajsAtThisRS: groupedTrajs) {

            for (Tuple2<Integer, Trajectory> trajectory: trajsAtThisRS){

                partitioning.put(trajectory._2().getTrajectoryID(),bucketNumber);
                sum++;
                if (sum>=bucketCapacity){
                    ++bucketNumber;
                    sum=0;
                }
            }

        }

        return partitioning;
    }

//    private static HashMap<Integer,Integer> assignTrajsToBuckets2(JavaRDD<Iterable<Tuple2<Integer, Trajectory>>> groupedTrajs, int bucketCapacity, JavaSparkContext sc) {
//        HashMap<Long,Long> partitioning = new HashMap<>();
//        LongAccumulator sum = sc.sc().longAccumulator("NofQueries");
//        LongAccumulator bucketNumber = sc.sc().longAccumulator("NofQueries");
//
//
//        groupedTrajs.map(new Function<Iterable<Tuple2<Integer,Trajectory>>, HashMap<Long,Long>>() {
//            @Override
//            public HashMap<Long, Long> call(Iterable<Tuple2<Integer, Trajectory>> v1) throws Exception {
//                List<Tuple2<Integer, Trajectory>> trajsAtThisTimestamp = Lists.newArrayList(tuple2s);
//
//
//                for (Tuple2<Integer, Trajectory> trajectory: trajsAtThisTimestamp){
//
//                    partitioning.put(trajectory._2().getTrajectoryID(),bucketNumber.value());
//                    sum.add(1l);
//                    if (sum>=bucketCapacity){
//                        ++bucketNumber;
//                        sum=0;
//                    }
//                }
//            }
//        })
//        groupedTrajs.foreach(new VoidFunction<Iterable<Tuple2<Integer, Trajectory>>>() {
//            @Override
//            public void call(Iterable<Tuple2<Integer, Trajectory>> tuple2s) throws Exception {
//                List<Tuple2<Integer, Trajectory>> trajsAtThisTimestamp = Lists.newArrayList(tuple2s);
//
//
//                for (Tuple2<Integer, Trajectory> trajectory: trajsAtThisTimestamp){
//
//                    partitioning.put(trajectory._2().getTrajectoryID(),bucketNumber.value());
//                    sum.add(1l);
//                    if (sum>=bucketCapacity){
//                        ++bucketNumber;
//                        sum=0;
//                    }
//                }
//
//
//            }
//        });
//
//        return partitioning;
//
//    }

    public static HashMap<Integer, Integer> sliceRSToBuckets2(JavaPairRDD<Integer, Trajectory> trajectoryDataset, final int bucketCapacity) {


        List<Iterable<Tuple2<Integer, Trajectory>>> groupedTrajs = trajectoryDataset.
                groupBy((Function<Tuple2<Integer, Trajectory>, Integer>) v1 -> v1._2().getStartingRS()).values().collect();

        HashMap<Integer,Integer> partitioning = assignTrajsToBuckets(groupedTrajs, bucketCapacity);



        return partitioning;



    }

//    public static HashMap<Integer, Integer> sliceRSToBuckets3(JavaPairRDD<Integer, Trajectory> trajectoryDataset, final int bucketCapacity, JavaSparkContext sc) {
//
//
//        JavaRDD<Iterable<Tuple2<Integer, Trajectory>>> groupedTrajs = trajectoryDataset.
//                groupBy((Function<Tuple2<Integer, Trajectory>, Integer>) v1 -> v1._2().getStartingRS()).values();
//
//        HashMap<Integer,Integer> partitioning = assignTrajsToBuckets2(groupedTrajs, bucketCapacity, sc);;
//
//
//
//        return partitioning;
//
//
//
//    }




    /**
     * Used by Time Slicing assignTrajsToBuckets
     *
     * @param trajectoryDataset
     * @param bucketCapacity
     * @return
     */
    public static Map<Long, List<Integer>> sliceTSToBuckets(JavaPairRDD<Integer, Trajectory> trajectoryDataset, final int bucketCapacity) {

        // we sort by key so neighbouring timestamps fall within the same bucket
//        Map<Long, Integer> sortedByTimestamp = trajectoryDataset.mapToPair(traj -> new Tuple2<>(traj._2().getStartingTime(), 1)).reduceByKey((v1, v2) -> v1 + v2).sortByKey(true).collectAsMap();
        List<Tuple2<Long, Integer>> sortedByTimestamp = trajectoryDataset.mapToPair(traj -> new Tuple2<>(traj._2().getStartingTime(), 1)). reduceByKey((v1, v2) -> v1 + v2).sortByKey(true).collect();

        List<Tuple2<Long, Iterable<Tuple2<Integer, Trajectory>>>> kk=trajectoryDataset.
                groupBy(new Function<Tuple2<Integer,Trajectory>, Long>() {
            @Override
            public Long call(Tuple2<Integer, Trajectory> v1) throws Exception {
                return v1._2().getStartingTime();
            }
        }).collect();


        Map<Long, List<Integer>> partitioning = new TreeMap<>();
        int sum = 0;
        int bucketNumber = 0;
        for (Tuple2<Long, Integer> entry : sortedByTimestamp) {

//            System.out.println("trajFreq:"+entry._2());
            //get trajectory frequency at this timestamp
            sum += entry._2();

            List<Integer> buckets = new ArrayList<>();

            if (sum >= bucketCapacity) {
                //capacity exceeded

                //empty space left in the bucket
                int emptySpaceLeft = (bucketCapacity) - sum;
                //trajectories left to assign in this bucket
                int trajectoriesLeft = entry._2() - emptySpaceLeft;

//                System.out.println("trajectoriesLeft:"+trajectoriesLeft);
//                partitioning.put(entry._1(), bucketNumber);

                int assignTo = (int) Math.ceil((double) trajectoriesLeft / bucketCapacity);

                for (int i = 0; i < assignTo; i++) {
                    buckets.add(++bucketNumber);
//                    TODO trajectoriesLeft has to change here

                }
                trajectoriesLeft = trajectoriesLeft - (assignTo * bucketCapacity);


                if (trajectoriesLeft > 0) {
                    buckets.add(++bucketNumber);
                }
                sum = 0;
                //assign to the next bucket
//                ++bucketNumber;
            } else {
                //assign this timestamp to this bucket
                //TODO assign timestamp to multiple buckets so partitioning variable should be Map<Long, List<Integer>>
                //TODO make bucketing algorithm 'perfect' so buckets are completely filled
                buckets.add(bucketNumber);
            }
            partitioning.merge(entry._1(), buckets, (list1, list2) -> Stream.of(list1, list2).flatMap(Collection::stream).collect(Collectors.toList()));

        }
        System.out.println("nofTrajsIs:" + trajectoryDataset.count());
        int nofPartitions = Math.toIntExact(partitioning.values().stream().distinct().count());
        System.out.println("whereas partitioning map is:" + nofPartitions);

        return partitioning;

    }

    /**
     * Used by Vertical assignTrajsToBuckets
     *
     * @param trajectoryDataset
     * @param bucketCapacity
     * @return
     */
    public static Map<Integer, Integer> sliceRSToBuckets(JavaPairRDD<Integer, Trajectory> trajectoryDataset, final int bucketCapacity) {

        //could sort this to get a better bucketing
        Map<Integer, Integer> sortedByTrajFreq = trajectoryDataset.mapToPair(traj -> new Tuple2<>(traj._2().getStartingRS(), 1)).reduceByKey((v1, v2) -> v1 + v2).collectAsMap();

        //sort by timestamp for time slicing
        int frequencyTotal = sortedByTrajFreq.values().stream().mapToInt(Integer::intValue).sum();

        Map<Integer, Integer> partitioning = new TreeMap<>();
        int sum = 0;
        int bucketNumber = 0;
        for (Map.Entry<Integer, Integer> entry : sortedByTrajFreq.entrySet()) {

            //get trajectory frequency
            sum += entry.getValue();

            if (sum >= bucketCapacity) {
                //capacity exceeded
//                System.out.println("capacityExceeded");
                //assign to the next bucket
                ++bucketNumber;
                sum = entry.getValue();
            }
            //assign this to partition, key is startingRoadSegment
            partitioning.put(entry.getKey(), bucketNumber);

        }
//        System.out.println("frequencyTotal:"+frequencyTotal);
//        System.out.println("partitionNumber:"+bucketNumber);
//        System.out.println("bucketCapacity:" + bucketCapacity);
//        System.out.println("expectedNofPartitions:" + frequencyTotal / bucketCapacity);
//        System.out.println("nofPartitions:" + partitioning.values().stream().distinct().count());

        return partitioning;

    }
}
