package utilities;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.*;

/**
 * Created by giannis on 29/03/19.
 */
public class Intervals {


    public static HashMap<Integer, Integer> sliceHorizontalToBuckets2(JavaPairRDD<Integer, Trajectory> trajectoryDataset, final int bucketCapacity) {

        //trajectory ids are sorted by length
        List<Integer> groupedTrajs = trajectoryDataset.mapToPair(new PairFunction<Tuple2<Integer, Trajectory>, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Trajectory> t) throws Exception {
                return new Tuple2<Integer, Integer>(t._2().getRoadSegments().size(), t._2().getTrajectoryID());
            }
        })
                .sortByKey().values().collect();


        HashMap<Integer, Integer> partitioning = assignTrajsToBuckets(groupedTrajs.iterator(), bucketCapacity);

//        HashMap<Integer,Integer> partitioning = assignTrajsToBucketsHorizontal(trajectoryDataset.collect(),bucketCapacity);


        return partitioning;


    }

    public static HashMap<Integer, Integer> sliceTSToBuckets2(JavaPairRDD<Integer, Trajectory> trajectoryDataset, final int bucketCapacity) {


        List<Integer> groupedTrajs = trajectoryDataset.
                mapToPair(new PairFunction<Tuple2<Integer, Trajectory>, Long, Integer>() {
                    @Override
                    public Tuple2<Long, Integer> call(Tuple2<Integer, Trajectory> t) throws Exception {
                        return new Tuple2<>(t._2().getStartingTime(), t._2().getTrajectoryID());
                    }
                }).sortByKey().values().collect();
        HashMap<Integer, Integer> partitioning = assignTrajsToBuckets(groupedTrajs.iterator(), bucketCapacity);


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

        List<Integer> groupedTrajs = trajectoryDataset.mapToPair(new PairFunction<Tuple2<Integer, Trajectory>, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Trajectory> tuple) throws Exception {
                return new Tuple2<>(tuple._2(). getStartingRS(), tuple._2().getTrajectoryID());
            }


        }).sortByKey().values().collect();

        HashMap<Integer, Integer> partitioning = assignTrajsToBuckets(groupedTrajs.iterator(), bucketCapacity);


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

        return partitioning;

    }

    public static HashMap<Integer, Integer> hybridSlicing(JavaPairRDD<Integer, Trajectory> trajectoryDataset, final int bucketCapacity) {


        List<Integer> groupedTrajs = trajectoryDataset.mapToPair(new PairFunction<Tuple2<Integer, Trajectory>, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Trajectory> tuple) throws Exception {
                return new Tuple2<>(tuple._1(), tuple._2().getTrajectoryID());
            }


        }).sortByKey().values().collect();



        HashMap<Integer, Integer> partitioning = assignTrajsToBuckets(groupedTrajs.iterator(), bucketCapacity);

        return partitioning;
    }


    private static HashMap<Integer, Integer> assignTrajsToBuckets(Iterator<Integer> sortedTrajs, final int bucketCapacity) {

        HashMap<Integer, Integer> partitioning = new HashMap<>();
        int sum = 0;
        int bucketNumber = 0;

        while (sortedTrajs.hasNext()) {


            partitioning.put(sortedTrajs.next(), bucketNumber);
            sum++;
            if (sum >= bucketCapacity) {
                ++bucketNumber;
                sum = 0;
            }

        }

        return partitioning;


    }
}
