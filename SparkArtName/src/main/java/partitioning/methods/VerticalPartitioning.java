package partitioning.methods;

import comparators.IntegerComparator;
import key.selectors.CSVTrajIDSelector;
import map.functions.CSVRecToTrajME;
import map.functions.LineToCSVRec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.LongAccumulator;
import projections.ProjectRoadSegments;
import scala.Tuple2;
import trie.Query;
import trie.Trie;
import utilities.*;

import java.util.*;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;


/**
 * Created by giannis on 10/12/18.
 */
public class VerticalPartitioning {


    private static Map<Integer, Integer> sliceRSToBuckets(JavaPairRDD<Integer, Trajectory> trajectoryDataset, int bucketSize) {

//        final int bucketCapacity = trajectoryDataset.mapToPair(traj -> new Tuple2<>(traj._2().getStartingRS(), 1)).reduceByKey((v1, v2) -> v1 + v2).mapToPair(pair -> new Tuple2<>(pair._2(), pair._1())).sortByKey(false, 1).first()._1();
        final int bucketCapacity = bucketSize;//trajectoryDataset.mapToPair(traj -> new Tuple2<>(traj._2().getStartingRS(), 1)).reduceByKey((v1, v2) -> v1 + v2).mapToPair(pair -> new Tuple2<>(pair._2(), pair._1())).sortByKey(false, 1).first()._1();

        //could sort this to get a better bucketing
        Map<Integer, Integer> sortedByTrajFreq = trajectoryDataset.mapToPair(traj -> new Tuple2<>(traj._2().getStartingRS(), 1)).reduceByKey((v1, v2) -> v1 + v2).collectAsMap();

        int frequencyTotal=sortedByTrajFreq.values().stream().mapToInt(Integer::intValue).sum();
        System.out.println("nofStartingRoadSegments:"+trajectoryDataset.mapToPair(traj -> new Tuple2<>(traj._2().getStartingRS(), 1)).keys().distinct().count());

        Map<Integer, Integer> partitioning = new TreeMap<>();
        int sum = 0;
        int partitionNumber = 0;
        for (Map.Entry<Integer, Integer> entry : sortedByTrajFreq.entrySet()) {

            //get trajectory frequency
            sum += entry.getValue();

            if (sum >= bucketCapacity) {
                //capacity exceeded
//                System.out.println("capacityExceeded");
                //assign to the next partition
                ++partitionNumber;
                sum = entry.getValue();
            }
            //assign this to partition
            partitioning.put(entry.getKey(), partitionNumber);

        }
        System.out.println("frequencyTotal:"+frequencyTotal);
        System.out.println("partitionNumber:"+partitionNumber);
        System.out.println("bucketCapacity:" + bucketCapacity);
        System.out.println("expectedNofPartitions:" + frequencyTotal / bucketCapacity);
        System.out.println("nofPartitions:" + partitioning.values().stream().distinct().count());

        return partitioning;

    }


    private static List<Integer> sliceNofSegments2(JavaRDD<CSVRecord> records, int nofVerticalSlices) {
        JavaRDD<Integer> roadSegments = records.map(new ProjectRoadSegments()).distinct().sortBy(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                return v1;
            }
        }, true, 1);

        int min = roadSegments.min(new IntegerComparator());
        int max = roadSegments.max(new IntegerComparator());

        long roadRange = max - min;

        long interval = roadRange / nofVerticalSlices;

        List<Integer> roadIntervals = new ArrayList<>();
        for (int i = min; i < max; i += interval) {
            roadIntervals.add(i);
        }
        roadIntervals.remove(roadIntervals.size() - 1);
        roadIntervals.add(max);


        return roadIntervals;
    }


    private static List<Integer> sliceNofSegments(JavaRDD<CSVRecord> records, int nofVerticalSlices) {
        JavaRDD<Integer> roadSegments = records.map(new ProjectRoadSegments()).distinct().sortBy(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                return v1;
            }
        }, true, 1);
        long nofSegments = roadSegments.count();
//        roadSegments.zipWithIndex().
        JavaPairRDD<Long, Integer> roadSegmentswIndex =
                roadSegments.zipWithIndex().mapToPair(new PairFunction<Tuple2<Integer, Long>, Long, Integer>() {
                    @Override
                    public Tuple2<Long, Integer> call(Tuple2<Integer, Long> t) throws Exception {
                        //t._2() is the road segment
                        return new Tuple2<>(t._2(), t._1());
                    }
                });
//        roadSegmentswIndex.foreach(t -> System.out.println(t));

        long bucketCapacity = nofSegments / nofVerticalSlices;
        List<Long> indices = new ArrayList<>();
        for (long i = 0; i < nofSegments - 1; i += bucketCapacity) {
            indices.add(i);
        }
        indices.add(nofSegments - 1);
        List<Integer> roadIntervals = new ArrayList<>();
        for (Long l : indices) {
            roadIntervals.add(roadSegmentswIndex.lookup(l).get(0));
        }
        return roadIntervals;
    }


    public static void main(String[] args) {


        int nofVerticalSlices = Integer.parseInt(args[0]);
        String fileName = "file:////mnt/hgfs/VM_SHARED/trajDatasets/concatTrajectoryDataset.csv";
        String queryFile = "file:////mnt/hgfs/VM_SHARED/trajDatasets/onesix.csv";
//        String fileName = "file:///mnt/hgfs/VM_SHARED/samplePort.csv";
//          String fileName= "file:////mnt/hgfs/VM_SHARED/trajDatasets/half.csv";
//        String fileName= "hdfs:////half.csv";
//        String fileName= "hdfs:////85TD.csv";


//        String fileName= "file:////home/giannis/octant.csv";
//        String fileName = "file:////mnt/hgfs/VM_SHARED/trajDatasets/onesix.csv";
//        String fileName = "file:///mnt/hgfs/VM_SHARED/samplePort.csv";
//        String queryFile = "file:///mnt/hgfs/VM_SHARED/samplePort.csv";;
//        String queryFile = "file:///mnt/hgfs/VM_SHARED/samplePort.csv";
//        String queryFile = "file:///mnt/hgfs/VM_SHARED/trajDatasets/queryRecords.csv";
//        String queryFile = "file:////data/queryRecords.csv";
//        String fileName= "hdfs:////synth5GBDataset.csv";
//        String queryFile = "hdfs:////queryRecords.csv";
//        String queryFile = "hdfs:////200KqueryRecords.csv";
//        String queryFile = "hdfs:////queryRecordsOnesix.csv";
//        String fileName = "hdfs:////concatTrajectoryDataset.csv";
//        String queryFile = "hdfs:////200KqueryRecords.csv";
//        String fileName= "hdfs:////onesix.csv";
//        String queryFile = "hdfs:////onesix.csv";
        SparkConf conf = new SparkConf().setAppName(VerticalPartitioning.class.getSimpleName() + nofVerticalSlices)
                .setMaster("local[*]")
                .set("spark.executor.instances", "" + Parallelism.PARALLELISM);
//        SparkConf conf = new SparkConf().setAppName(VerticalPartitioning.class.getSimpleName() + nofVerticalSlices);

        JavaSparkContext sc = new JavaSparkContext(conf);
        LongAccumulator queryLengthSum = sc.sc().longAccumulator("queryLengthSum");
        JavaRDD<CSVRecord> records = sc.textFile(fileName).map(new LineToCSVRec());
        DoubleAccumulator joinTimeAcc = sc.sc().doubleAccumulator("joinTimeAcc");

//        JavaRDD<Long> roadSegments = records.map(new ProjectRoadSegments()).sortBy(null,true,1);


//        List<Integer> roadIntervals = sliceNofSegments(records, nofVerticalSlices);
//        System.out.println("roadIntervals.size:" + roadIntervals.size());
//        System.out.println("roadIntervals:" + roadIntervals);

        JavaPairRDD<Integer, CSVRecord> queryRecords = sc.textFile(queryFile, Parallelism.PARALLELISM).map(new LineToCSVRec()).mapToPair(csvRec -> new Tuple2<>(csvRec.getTrajID(), csvRec));


        JavaPairRDD<Integer, Trajectory> trajectoryDataset = records.groupBy(new CSVTrajIDSelector()).mapValues(new CSVRecToTrajME());

//        TrajectorySynthesizer.tim
//        .mapToPair(new PairFunction<Tuple2<Integer, Trajectory>, Integer, Trajectory>() {
//            @Override
//            public Tuple2<Integer, Trajectory> call(Tuple2<Integer, Trajectory> trajectoryTuple2) throws Exception {
//                int x = new StartingRSPartitioner(roadIntervals, nofVerticalSlices).getPartition2(trajectoryTuple2._2().getStartingRS());
//                trajectoryTuple2._2().setVerticalID(x);
//                int x;
//                return new Tuple2<Integer, Trajectory>(x, trajectoryTuple2._2());
//                return new Tuple2<Integer, Trajectory>(trajectoryTuple2._2().getStartingRS(), trajectoryTuple2._2());
//            }
//        });

        Map<Integer, Integer> buckets = sliceRSToBuckets(trajectoryDataset,nofVerticalSlices);

        trajectoryDataset = trajectoryDataset.mapToPair(trajectory -> {
            Integer x = buckets.get(trajectory._2().getStartingRS());

            trajectory._2().setVerticalID(x);
            return new Tuple2<>(trajectory._2().getVerticalID(), trajectory._2());
        });

        JavaPairRDD<Integer, Query> queries =
                queryRecords.groupByKey().mapValues(new CSVRecToTrajME()).map(t -> new Query(t._2())).mapToPair(q -> {
                    q.setVerticalID(buckets.get(q.getStartingRoadSegment()));
                    return new Tuple2<>(q.getVerticalID(), q);
                });
//        filter(q -> buckets.get(q.getStartingRoadSegment())==null?false:true ).


//        JavaPairRDD<Integer, Trajectory>trajectoryDataset.mapToPair(trajectory -> new Tuple2<>(trajectory._2().getVerticalID(),trajectory));


        Stats.nofTrajsInEachSlice(trajectoryDataset, PartitioningMethods.VERTICAL);
        Stats.nofQueriesInSlice(queries, PartitioningMethods.VERTICAL);
        System.out.println("Done. Trajectories build");


        System.out.println("nofBuckets:" + buckets.size());
//        List<Integer> partitions = IntStream.range(0, nofVerticalSlices).boxed().collect(toList());
        List<Integer> partitions = IntStream.range(0, buckets.size()).boxed().collect(toList());
        JavaRDD<Integer> partitionsRDD = sc.parallelize(partitions);
        JavaPairRDD<Integer, Trie> emptyTrieRDD = partitionsRDD.mapToPair(new PairFunction<Integer, Integer, Trie>() {
            @Override
            public Tuple2<Integer, Trie> call(Integer verticalID) throws Exception {
                Trie t = new Trie();
                t.setVerticalID(verticalID);
                return new Tuple2<>(verticalID, t);
            }
        });

        Stats.nofQueriesOnEachNode(queries, PartitioningMethods.VERTICAL);
        Stats.nofTrajsOnEachNode(trajectoryDataset, PartitioningMethods.VERTICAL);

        JavaPairRDD<Integer, Trie> trieRDD =
                emptyTrieRDD.groupWith(trajectoryDataset).mapValues(new Function<Tuple2<Iterable<Trie>, Iterable<Trajectory>>, Trie>() {
                    @Override
                    public Trie call(Tuple2<Iterable<Trie>, Iterable<Trajectory>> v1) throws Exception {
                        Trie trie = v1._1().iterator().next();
                        for (Trajectory trajectory : v1._2()) {
                            trie.insertTrajectory2(trajectory);
                        }
                        return trie;
                    }
                });


        System.out.println("nofTries:" + trieRDD.count());


        System.out.println("TrieNofPartitions:" + trieRDD.rdd().partitions().length);

//
//        List<Set<Integer>> collectedResult = resultSet.values().collect();
//        collectedResult.stream().forEach(System.out::println);
//        System.out.println("resultSetSize:" + collectedResult.size());


        Broadcast<List<Tuple2<Integer, Query>>> partBroadQueries = sc.broadcast(queries.collect());
        JavaRDD<Set<Integer>> resultSet =
                trieRDD.map(new Function<Tuple2<Integer, Trie>, Set<Integer>>() {
                    @Override
                    public Set<Integer> call(Tuple2<Integer, Trie> v1) throws Exception {
                        List<Tuple2<Integer, Query>> localQueries = partBroadQueries.value();
                        Set<Integer> answer = new TreeSet<>();

                        for (Tuple2<Integer, Query> queryEntry : localQueries) {
//                            if (v1._2().getStartingRS() == queryEntry._2().getStartingRoadSegment()) {
                            if (v1._2().getVerticalID() == queryEntry._2().getVerticalID()) {
                                long t1 = System.nanoTime();
                                Set<Integer> ans = v1._2().queryIndex(queryEntry._2());
                                long t2 = System.nanoTime();
                                long diff = t2 - t1;
                                joinTimeAcc.add(diff * Math.pow(10.0, -9.0));
                                answer.addAll(ans);
                            }
                        }
                        return answer;
                    }
                }).filter(set -> set.isEmpty() ? false : true);

        List<Integer> collectedResult = resultSet.collect().stream().flatMap(set -> set.stream()).collect(toList());


//        List<Integer> collectedResult = resultSet.collect();
        for (Integer t : collectedResult) {
            System.out.println(t);
        }
        System.out.println("resultSetSize:" + collectedResult.size());

        sc.stop();
        sc.close();


        System.out.println("joinTime(sec):" + joinTimeAcc.value());


    }


}
