package partitioning.methods;

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
import scala.Tuple2;
import trie.Query;
import trie.Trie;
import utilities.*;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;


/**
 * Created by giannis on 10/12/18.
 */
public class VerticalPartitioning {

    public static void main(String[] args) {


        int bucketCapacity = Integer.parseInt(args[0]);
        String fileName = "hdfs:////timeSkewedDataset.csv";
        String queryFile = "hdfs:////timeSkewedQueries";
//        String fileName = "hdfs:////rssd.csv";
//        String queryFile = "hdfs:////rssq.csv";

//        String fileName = "hdfs:////roadSegmentSkewedDataset.csv";
//        String queryFile = "hdfs:////roadSegmentSkewedQueries";
//        String fileName = "file:////mnt/hgfs/VM_SHARED/trajDatasets/concatTrajectoryDataset.csv";
//        String fileName = "file:////mnt/hgfs/VM_SHARED/trajDatasets/onesix.csv";
//        String queryFile = "file:////mnt/hgfs/VM_SHARED/trajDatasets/onesix.csv";
//        String fileName = "file:///mnt/hgfs/VM_SHARED/samplePort.csv";

//        String queryFile = "file:///mnt/hgfs/VM_SHARED/trajDatasets/queryRecords.csv";
//        String queryFile = "file:////data/queryRecords.csv";
//        String fileName = "hdfs:////concatTrajectoryDataset.csv";
//        String queryFile = "hdfs:////concatTrajectoryQueries";
//        SparkConf conf = new SparkConf().setAppName(VerticalPartitioning.class.getSimpleName() + bucketCapacity)
//                .setMaster("local[*]")
//                .set("spark.executor.instances", "" + Parallelism.PARALLELISM);
        SparkConf conf = new SparkConf().
                setAppName(VerticalPartitioning.class.getSimpleName() + bucketCapacity);

        JavaSparkContext sc = new JavaSparkContext(conf);
        LongAccumulator queryLengthSum = sc.sc().longAccumulator("queryLengthSum");
        JavaRDD<CSVRecord> records = sc.textFile(fileName).map(new LineToCSVRec());
        DoubleAccumulator joinTimeAcc = sc.sc().doubleAccumulator("joinTimeAcc");
        DoubleAccumulator avgIndexDepth = sc.sc().doubleAccumulator("avgIndexDepth");
        DoubleAccumulator avgIndexWidth = sc.sc().doubleAccumulator("avgIndexWidth");


        JavaPairRDD<Integer, CSVRecord> queryRecords = sc.textFile(queryFile, Parallelism.PARALLELISM).map(new LineToCSVRec()).mapToPair(csvRec -> new Tuple2<>(csvRec.getTrajID(), csvRec));


        JavaPairRDD<Integer, Trajectory> trajectoryDataset = records.groupBy(csv -> csv.getTrajID()).mapValues(new CSVRecToTrajME());

        Map<Integer, Integer> buckets = Intervals.sliceRSToBuckets2(trajectoryDataset, bucketCapacity);

        trajectoryDataset = trajectoryDataset.mapToPair(trajectory -> {
            Integer bucket = buckets.get(trajectory._2().getTrajectoryID());

            trajectory._2().setPartitionID(bucket);
            return new Tuple2<>(trajectory._2().getPartitionID(), trajectory._2());
        });

        JavaPairRDD<Integer, Query> queries =
                queryRecords.groupByKey().mapValues(new CSVRecToTrajME()).map(t -> new Query(t._2())).mapToPair(q -> {
//                    q.setPartitionID(buckets.get(q.getStartingRoadSegment()));
                    return new Tuple2<>(q.getPartitionID(), q);
                });
//        filter(q -> buckets.get(q.getStartingRoadSegment())==null?false:true ).


//        JavaPairRDD<Integer, Trajectory>trajectoryDataset.mapToPair(trajectory -> new Tuple2<>(trajectory._2().getPartitionID(),trajectory));


//        Stats.nofTrajsInEachSlice(trajectoryDataset, PartitioningMethods.VERTICAL);
//        Stats.nofQueriesInSlice(queries, PartitioningMethods.VERTICAL);
        System.out.println("Done. Trajectories build");


        int nofPartitions = Math.toIntExact(buckets.values().stream().distinct().count());
        System.out.println("nofBuckets:" + nofPartitions);
//        List<Integer> partitions = IntStream.range(0, nofVerticalSlices).boxed().collect(toList());
        List<Integer> partitions = IntStream.range(0, nofPartitions).boxed().collect(toList());
        JavaRDD<Integer> partitionsRDD = sc.parallelize(partitions);
        JavaPairRDD<Integer, Trie> emptyTrieRDD = partitionsRDD.mapToPair(new PairFunction<Integer, Integer, Trie>() {
            @Override
            public Tuple2<Integer, Trie> call(Integer partitionID) throws Exception {
                Trie t = new Trie();
                t.setPartitionID(partitionID);
                return new Tuple2<>(partitionID, t);
            }
        });

//        Stats.nofQueriesOnEachNode(queries, PartitioningMethods.VERTICAL);
        Stats.nofTrajsOnEachNode(trajectoryDataset, sc);

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


        long noftries = trieRDD.count();
        System.out.println("nofTries:" + noftries);
        System.out.println("partitions:"+partitions.size());



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

                        avgIndexDepth.add(v1._2().avgIndexDepth());
                        avgIndexWidth.add(v1._2().avgIndexWidth());
                        for (Tuple2<Integer, Query> queryEntry : localQueries) {
//                            if (v1._2().getStartingRS() == queryEntry._2().getStartingRoadSegment()) {
//                            if (v1._2().getPartitionID() == queryEntry._2().getPartitionID()) {
                            long t1 = System.nanoTime();
                            Set<Integer> ans = v1._2().queryIndex(queryEntry._2());
                            long t2 = System.nanoTime();
                            if (!ans.isEmpty()) {
//                                System.out.println("iid:" + v1._1() + "," + ans.size());
                            }

                            long diff = t2 - t1;
                            joinTimeAcc.add(diff * Math.pow(10.0, -9.0));
                            answer.addAll(ans);
//                            }
                        }
                        return answer;
                    }
                }).filter(set -> set.isEmpty() ? false : true);

        List<Integer> collectedResult = resultSet.collect().stream().flatMap(set -> set.stream()).distinct().collect(toList());

        for (Integer t : collectedResult) {
            System.out.println(t);
        }
        System.out.println("resultSetSize:" + collectedResult.size());

        sc.stop();
        sc.close();


        System.out.println("joinTime(sec):" + joinTimeAcc.value());
        System.out.println("avgTrieDepth:" + avgIndexDepth.value() / noftries);
        System.out.println("avgIndexWidth:"+avgIndexWidth.value() / noftries);

    }


}
