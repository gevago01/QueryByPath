package partitioning.methods;

import map.functions.CSVRecToTrajME;
import map.functions.LineToCSVRec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.LongAccumulator;
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
public class TimeSlicing {


    public static void main(String[] args) {
        int numberOfSlices = Integer.parseInt(args[0]);

        String appName = TimeSlicing.class.getSimpleName() + numberOfSlices;


//        String fileName = "hdfs:////rssd.csv";
//        String queryFile = "hdfs:////rssq.csv";

//        String fileName = "hdfs:////timeSkewedDataset.csv";
//        String queryFile = "hdfs:////timeSkewedQueries";
//        String fileName = "hdfs:////roadSegmentSkewedDataset.csv";
//        String queryFile = "hdfs:////roadSegmentSkewedQueries";

//        String fileName = "file:////home/giannis/IdeaProjects/SparkTrajectories/SparkArtName/timeSkewedDataset.csv";
//        String queryFile = "file:////home/giannis/IdeaProjects/SparkTrajectories/SparkArtName/timeSkewedDataset.csv";
//        String queryFile = "file:////timeSkewedDataset";
//        String fileName = "file:///mnt/hgfs/VM_SHARED/samplePort.csv";
//        String queryFile = "file:///mnt/hgfs/VM_SHARED/samplePort.csv";

//        String fileName = "file:///mnt/hgfs/VM_SHARED/trajDatasets/concatTrajectoryDataset.csv";
//        String queryFile = "file:///mnt/hgfs/VM_SHARED/trajDatasets/concatTrajectoryDataset.csv";
        String fileName = "file:///mnt/hgfs/VM_SHARED/trajDatasets/onesix.csv";
        String queryFile = "file:///mnt/hgfs/VM_SHARED/trajDatasets/onesix.csv";
//        String queryFile = "file:///mnt/hgfs/VM_SHARED/trajDatasets/onesix.csv";
//        String queryFile = "file:///mnt/hgfs/VM_SHARED/trajDatasets/queryRecords.csv";

//        String fileName = "hdfs:////concatTrajectoryDataset.csv";
//        String queryFile = "hdfs:////concatTrajectoryQueries";
//        String queryFile = "hdfs:////queryRecordsOnesix.csv";



        SparkConf conf = new SparkConf().setAppName(appName).setMaster("local[*]")
                .set("spark.executor.instances", "" + Parallelism.PARALLELISM)
                .set("spark.driver.maxResultSize", "3G");
//        SparkConf conf = new SparkConf().
//                setAppName(appName);


        JavaSparkContext sc = new JavaSparkContext(conf);
//        sc.setCheckpointDir("/home/giannis/IdeaProjects/SparkTrajectories/SparkArtName/target/cpdir");
        LongAccumulator nofQueries = sc.sc().longAccumulator("NofQueries");
        DoubleAccumulator joinTimeAcc = sc.sc().doubleAccumulator("joinTimeAcc");
        DoubleAccumulator avgIndexDepth = sc.sc().doubleAccumulator("avgIndexDepth");
        DoubleAccumulator avgIndexWidth = sc.sc().doubleAccumulator("avgIndexWidth");


        JavaRDD<CSVRecord> records = sc.textFile(fileName).map(new LineToCSVRec());



        JavaPairRDD<Integer, Trajectory> trajectoryDataset = records.groupBy(csv -> csv.getTrajID()).mapValues(new CSVRecToTrajME()).mapToPair(pair -> new Tuple2<>(pair._2().getPartitionID(), pair._2()));//filter(new ReduceNofTrajectories());


        HashMap<Integer, Integer> buckets = Intervals.sliceTSToBuckets2(trajectoryDataset, numberOfSlices);

//        buckets.
        trajectoryDataset = trajectoryDataset.mapToPair(trajectory -> {
            Integer bucket = buckets.get(trajectory._2().getTrajectoryID());

            trajectory._2().setPartitionID(bucket);
            return new Tuple2<>(trajectory._2().getPartitionID(), trajectory._2());
        });

        Stats.nofTrajsOnEachNode(trajectoryDataset, PartitioningMethods.TIME_SLICING);
//        Stats.nofTrajsInEachSlice(trajectoryDataset, PartitioningMethods.TIME_SLICING);

        //records RDD not needed any more
        records.unpersist(true);
//        GroupSizes.mesaureGroupSize(trajectoryDataset, TimeSlicing.class.getName());


        int nofBuckets = Math.toIntExact(buckets.values().stream().distinct().count());
        System.out.println("nofBuckets:" + nofBuckets);
        List<Integer> partitions = IntStream.range(0, nofBuckets).boxed().collect(toList());
        JavaRDD<Integer> partitionsRDD = sc.parallelize(partitions);
        JavaPairRDD<Integer, Trie> emptyTrieRDD = partitionsRDD.mapToPair(new PairFunction<Integer, Integer, Trie>() {
            @Override
            public Tuple2<Integer, Trie> call(Integer partitionID) throws Exception {
                Trie t = new Trie();
                t.setPartitionID(partitionID);
                return new Tuple2<>(partitionID, t);
            }
        });

        assert (partitions.size() == numberOfSlices && partitionsRDD.count() == numberOfSlices);


//        emptyTrieRDD.foreach(new VoidFunction<Tuple2<Integer, Trie>>() {
//            @Override
//            public void call(Tuple2<Integer, Trie> integerTrieTuple2) throws Exception {
//                System.out.println("emtid:"+integerTrieTuple2._1());
//            }
//        });
//        trajectoryDataset.keys().distinct().foreach(new VoidFunction<Integer>() {
//            @Override
//            public void call(Integer integer) throws Exception {
//                System.out.println("tid:"+integer);
//            }
//        });


//        if (setDifference.count()>0){
//            System.err.println("setDifference is not empty");
//            System.exit(1);
//        }

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


        //trajectoryDataset RDD not needed any more
        trajectoryDataset.unpersist(true);

        long noftries=trieRDD.count();
        System.out.println("noftries:"+noftries);

        trieRDD.foreach(new VoidFunction<Tuple2<Integer, Trie>>() {
            @Override
            public void call(Tuple2<Integer, Trie> integerTrieTuple2) throws Exception {
                System.out.println("trajCounter:"+integerTrieTuple2._2.getTrajectoryCounter());
            }
        });

        System.out.println("Done. Tries build");
//        Query q = new Query(trajectory.getStartingTime(), trajectory.getEndingTime(), trajectory.roadSegments);
//        List<Integer> times_slices = q.determineTimeSlice(timePeriods);
//        q.partitionID = times_slices.get(new Random().nextInt(times_slices.size()));


        JavaPairRDD<Integer, CSVRecord> queryRecords = sc.textFile(queryFile).map(new LineToCSVRec()).mapToPair(csvRec -> new Tuple2<>(csvRec.getTrajID(), csvRec));
        JavaPairRDD<Integer, Query> queries =
                queryRecords.groupByKey().mapValues(new CSVRecToTrajME()).map(t -> new Query(t._2())).mapToPair(q -> {

//                    q.setPartitionID(buckets.getOrDefault(q.getQueryID(),1));
                    return new Tuple2<>(q.getPartitionID(), q);
                });
//        Broadcast<Map<Integer, Query>> broadcastedQueries = sc.broadcast(queries.collectAsMap());
        System.out.println("nofQueries:" + queries.count());
//        Stats.nofQueriesInSlice(queries, PartitioningMethods.TIME_SLICING);
        System.out.println("appName:" + appName);

        System.out.println("TrieNofPartitions:" + trieRDD.rdd().partitions().length);

        System.out.println("TrieNofPartitions:" + trieRDD.rdd().partitions().length);
//        Stats.nofQueriesOnEachNode(queries, PartitioningMethods.TIME_SLICING);
//        Stats.nofQueriesInSlice(queries);
//        Stats.nofTriesInPartitions(trieRDD);
        Broadcast<List<Tuple2<Integer, Query>>> partBroadQueries = sc.broadcast(queries.collect());


        JavaRDD<Set<Integer>> resultSetRDD =
              trieRDD.map(new Function<Tuple2<Integer, Trie>, Set<Integer>>() {
                    @Override
                    public Set<Integer> call(Tuple2<Integer, Trie> v1) throws Exception {
                        Set<Integer> answer = new TreeSet<>();

                        List<Tuple2<Integer, Query>> localQueries = partBroadQueries.value();

                        avgIndexDepth.add(v1._2().avgIndexDepth());
                        avgIndexWidth.add(v1._2().avgIndexWidth());
                        for (Tuple2<Integer, Query> queryEntry : localQueries) {

                                long t1 = System.nanoTime();
                                Set<Integer> ans = v1._2().queryIndex(queryEntry._2());
                                long t2 = System.nanoTime();
                                if (!ans.isEmpty()){
//                                    System.out.println("iid:"+v1._1()+","+ans.size());
                                }
                                long diff = t2 - t1;
                                joinTimeAcc.add(diff * Math.pow(10.0, -9.0));
                                answer.addAll(ans);
                        }
                        return answer;
                    }
                }).filter(set -> set.isEmpty() ? false : true);
        List<Integer> collectedResult = resultSetRDD.collect().stream().flatMap(set -> set.stream()).distinct().collect(toList());

        for (Integer t : collectedResult) {
            System.out.println(t);
        }
        System.out.println("resultSetSize:" + collectedResult.size());
        System.out.println("avgTrieDepth:"+avgIndexDepth.value() / noftries);
        System.out.println("avgIndexWidth:"+avgIndexWidth.value() / noftries);


    }
}
