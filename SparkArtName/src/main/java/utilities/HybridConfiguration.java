package utilities;

import comparators.IntegerComparator;
import comparators.LongComparator;
import comparators.TupleIntIntComparator;
import map.functions.CSVRecToTrajME;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import projections.ProjectTimestamps;
import scala.Tuple2;

import java.util.Comparator;

/**
 * Created by giannis on 11/09/19.
 */
public class HybridConfiguration {
    public static int getBucketCapacityUpperBound() {
        return bucketCapacityUpperBound;
    }

    private static int bucketCapacityUpperBound;

    public static int getRt_upperBound() {
        return rt_upperBound;
    }

    public static int getRl_upperBound() {
        return rl_upperBound;
    }

    public static int getBucketCapacityLowerBound() {
        return bucketCapacityLowerBound;
    }

    public static String printConf() {
        return "HybridConfiguration{" +
                "  rt_upperBound:" + getRt_upperBound() +
                ", rl_upperBound:" + getRl_upperBound() +
                ", rs_upperBound:" + getRs_upperBound() +
                ", rt_lowerBound:" + getRt_lowerBound() +
                ", rl_lowerBound:" + getRl_lowerBound() +
                ", rs_lowerBound:" + getRs_lowerBound() +
                ", bucketCapacityLowerBound:" + getBucketCapacityLowerBound() +
                ", bucketCapacityUpperBound:" + getBucketCapacityUpperBound() +
                "}";
    }

    private static int rt_upperBound;
    private static int rs_upperBound;
    private static int rl_upperBound;

    public static int getRt_lowerBound() {
        return rt_lowerBound;
    }

    public static int getRs_lowerBound() {
        return rs_lowerBound;
    }

    public static int getRl_lowerBound() {
        return rl_lowerBound;
    }

    private static int rt_lowerBound;
    private static int rs_lowerBound;
    private static int rl_lowerBound;
    private static int bucketCapacityLowerBound;

    public static void configure(JavaRDD<CSVRecord> records, Integer skewnessFactor) {
        records.count();
        JavaRDD<Long> timestamps = records.map(new ProjectTimestamps());
        long minTimestamp = timestamps.min(new LongComparator());
        long maxTimestamp = timestamps.max(new LongComparator());
        JavaPairRDD<Integer, Trajectory> trajectoryDataset = records.groupBy(csv -> csv.getTrajID()).mapValues(new CSVRecToTrajME());


//        JavaPairRDD<Integer, Integer> lenToFreqs=trajectoryDataset.mapToPair(new PairFunction<Tuple2<Integer,Trajectory>, Integer, Integer>() {
//            @Override
//            public Tuple2<Integer, Integer> call(Tuple2<Integer, Trajectory> t) throws Exception {
//                return new Tuple2<>(t._2().getRoadSegments().size(),1);
//            }
//        }).reduceByKey((l1,l2) -> l1+l2);


//        Tuple2<Integer, Integer> commonLenToFreq=lenToFreqs.max(new TupleIntIntComparator());

        JavaPairRDD<Integer, Integer> startingRS_sum=trajectoryDataset.mapToPair(new PairFunction<Tuple2<Integer,Trajectory>, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Trajectory> t) throws Exception {
                return new Tuple2<>(t._2().getStartingRS(),1);
            }
        }).reduceByKey((l1,l2) -> l1+l2);

        int nofStartingRS= (int) startingRS_sum.keys().distinct().count();



//        JavaPairRDD<Integer, Trajectory> sth = trajectoryDataset.group
        int sumOfLengths = trajectoryDataset.values().map(t -> t.getRoadSegments().size()).reduce((l1,l2) -> l1+l2);
        int minLen = trajectoryDataset.values().map(t -> t.getRoadSegments().size()).min(new IntegerComparator());
        int maxLen = trajectoryDataset.values().map(t -> t.getRoadSegments().size()).max(new IntegerComparator());
        long sumOfDurations=trajectoryDataset.values().map(traj -> traj.getEndingTime() - traj.getStartingTime()).reduce((v1,v2) -> v1+v2).longValue();
        long maxTrajDuration=trajectoryDataset.values().map(traj -> traj.getEndingTime() - traj.getStartingTime()).max(new LongComparator());
//        long maxDuration=trajectoryDataset.values().map(traj -> traj.getEndingTime() - traj.getStartingTime()).max(new LongComparator());

        int nofTrajs = (int) trajectoryDataset.count();
        double avgDuration = sumOfDurations / nofTrajs;

        int avgNofTrajsFromSegment = nofTrajs / nofStartingRS;
        int maxNofTrajsFromSegment = startingRS_sum.values().max(new IntegerComparator());

//        System.out.println("avgNofTrajsFromSegment:"+avgNofTrajsFromSegment+", maxNofTrajsFromSegment:"+maxNofTrajsFromSegment);
        double avgLength = sumOfLengths / nofTrajs;

        rt_upperBound = (int) ((maxTimestamp - minTimestamp)/ avgDuration);
        rl_upperBound = (int) Math.ceil ((maxLen - minLen)/avgLength);
        rs_upperBound = nofTrajs/avgNofTrajsFromSegment;


        rt_lowerBound = (int) ((maxTimestamp - minTimestamp)/ maxTrajDuration);
        rl_lowerBound = (int) Math.ceil ((maxLen - minLen)/(double)maxLen);
        rs_lowerBound = (int) Math.ceil ((double)nofStartingRS/maxNofTrajsFromSegment);

//        bucketCapacityLowerBound = rt_upperBound / rl_upperBound;
//        if (skewnessFactor==0) {
//            skewnessFactor = (int) (commonLenToFreq._2() / (double) nofTrajs * 100.0);
//        }
//        skewnessFactor=1;
        System.out.println("skewnessFactor is:"+skewnessFactor);
        bucketCapacityLowerBound = (int) (((double)skewnessFactor/100.0)*nofTrajs/((double) Parallelism.PARALLELISM));
        bucketCapacityUpperBound = (int) (nofTrajs/((double) Parallelism.PARALLELISM));


        System.out.println(printConf());
        System.out.println("maxNofTrajsFromSegment:"+maxNofTrajsFromSegment+",nofStartingRS:"+nofStartingRS);
//        System.out.println("commonTrajLen:"+commonLenToFreq._1()+"commonLen.frequency:"+commonLenToFreq._2());

    }


    public static int getRs_upperBound() {
        return rs_upperBound;
    }
}
