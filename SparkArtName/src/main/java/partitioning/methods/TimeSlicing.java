package partitioning.methods;

import map.functions.AddTrajToTrie;
import map.functions.CSVRecToTrajME;
import map.functions.CSVRecordToTrajectory;
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

import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

/**
 * Created by giannis on 10/12/18.
 */
public class TimeSlicing {


    public static void main(String[] args) {
        String dataHdfsName = null;
        String queryHdfsName = null;
        int bucketCapacity = 0;
        if (args.length == 2) {
            dataHdfsName = args[0];
            queryHdfsName = args[1];
        } else if (args.length == 3) {
            dataHdfsName = args[0];
            queryHdfsName = args[1];
            bucketCapacity = Integer.parseInt(args[2]);
        }
        String fileName = "hdfs:////"+dataHdfsName;
        String queryFile = "hdfs:////"+queryHdfsName;
//        String fileName = "file:////mnt/hgfs/VM_SHARED/trajDatasets/tiny.csv";
//        String queryFile = "file:////mnt/hgfs/VM_SHARED/trajDatasets/qq2.csv";
//        String fileName = "file:////home/giannis/IdeaProjects/SparkTrajectories/SparkArtName/timeSkewedDataset.csv";
//        String queryFile = "file:////home/giannis/IdeaProjects/SparkTrajectories/SparkArtName/timeSkewedDataset.csv";
//        String queryFile = "file:////timeSkewedDataset";

//        String fileName = "file:///mnt/hgfs/VM_SHARED/trajDatasets/concatTrajectoryDataset.csv";
//        String queryFile = "file:///mnt/hgfs/VM_SHARED/trajDatasets/concatTrajectoryDataset.csv";
//        String fileName = "file:///mnt/hgfs/VM_SHARED/trajDatasets/onesix.csv";
//        String queryFile = "file:///mnt/hgfs/VM_SHARED/trajDatasets/onesix.csv";

//        String queryFile = "file:///mnt/hgfs/VM_SHARED/trajDatasets/queryRecords.csv";


//        SparkConf conf = new SparkConf().setMaster("local[*]")
//                .set("spark.executor.instances", "" + Parallelism.PARALLELISM)
//                .set("spark.driver.maxResultSize", "3G")
//                .setAppName(TimeSlicing.class.getSimpleName());
        SparkConf conf = new SparkConf().setAppName(TimeSlicing.class.getSimpleName() + bucketCapacity);


        JavaSparkContext sc = new JavaSparkContext(conf);
//        sc.setCheckpointDir("/home/giannis/IdeaProjects/SparkTrajectories/SparkArtName/target/cpdir");
        LongAccumulator nofQueries = sc.sc().longAccumulator("NofQueries");
        DoubleAccumulator joinTimeAcc = sc.sc().doubleAccumulator("joinTimeAcc");


        JavaRDD<CSVRecord> records = sc.textFile(fileName).map(new LineToCSVRec());

        Integer skewnessFactor;
        try {
            String skewness = dataHdfsName.substring(0, 2);
            skewnessFactor = Integer.parseInt(skewness);
        } catch (Exception e) {
            skewnessFactor = 1;
        }

        if (args.length != 3) {
            HybridConfiguration.configure(records, skewnessFactor);
            bucketCapacity = HybridConfiguration.getBucketCapacityLowerBound();
        }

        System.out.println("bucketCapacityIsNow:" + bucketCapacity);


        JavaPairRDD<Integer, Trajectory> trajectoryDataset = records.groupBy(csv -> csv.getTrajID()).mapValues(new CSVRecordToTrajectory());


        int nofTrajs = (int) trajectoryDataset.count();
        if (bucketCapacity > nofTrajs) {
            System.err.println("bucketCapacity>nofTrajs");
            System.exit(1);
        }
        HashMap<Integer, Integer> buckets = Intervals.sliceTSToBuckets2(trajectoryDataset, bucketCapacity);
        trajectoryDataset = trajectoryDataset.mapToPair(trajectory -> {
            Integer bucket = buckets.get(trajectory._2().getTrajectoryID());

            trajectory._2().setPartitionID(bucket);
            return new Tuple2<>(trajectory._2().getPartitionID(), trajectory._2());
        });

//        Stats.nofTrajsInEachSlice(trajectoryDataset, PartitioningMethods.TIME_SLICING);


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

        assert (partitions.size() == bucketCapacity && partitionsRDD.count() == bucketCapacity);


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

        JavaPairRDD<Integer, Trie> trieRDD = emptyTrieRDD.groupWith(trajectoryDataset).mapValues(new AddTrajToTrie());
        long noftries = trieRDD.count();
        System.out.println("nofTries:" + noftries);


        JavaPairRDD<Integer, CSVRecord> queryRecords = sc.textFile(queryFile).map(new LineToCSVRec()).mapToPair(csvRec -> new Tuple2<>(csvRec.getTrajID(), csvRec));
        JavaPairRDD<Integer, Query> queries =
                queryRecords.groupByKey().mapValues(new CSVRecToTrajME()).map(t -> new Query(t._2())).mapToPair(q -> {

//                    q.setPartitionID(buckets.getOrDefault(q.getQueryID(),1));
                    return new Tuple2<>(q.getPartitionID(), q);
                });
//        Broadcast<Map<Integer, Query>> broadcastedQueries = sc.broadcast(queries.collectAsMap());
        System.out.println("nofQueries:" + queries.count());

        Broadcast<List<Tuple2<Integer, Query>>> partBroadQueries = sc.broadcast(queries.collect());


        JavaRDD<Set<Integer>> resultSetRDD =
                trieRDD.map(new Function<Tuple2<Integer, Trie>, Set<Integer>>() {
                    @Override
                    public Set<Integer> call(Tuple2<Integer, Trie> v1) throws Exception {
                        List<Tuple2<Integer, Query>> localQueries = partBroadQueries.value();
                        Set<Integer> answer = new TreeSet<>();

                        for (Tuple2<Integer, Query> queryEntry : localQueries) {

                            Set<Integer> ans = null;
                            long t1 = System.nanoTime();
//            if (queryEntry._2().getStartingRoadSegment() >= v1._2().getMinStartingRS() && queryEntry._2().getStartingRoadSegment() <= v1._2().getMaxStartingRS()) {
                            if (queryEntry._2().getStartingTime() >= v1._2().getMinStartingTime() && queryEntry._2().getStartingTime() <= v1._2().getMaxStartingTime()) {
                                ans = v1._2().queryIndex(queryEntry._2());
                            }
//            }
                            if (ans != null) {
                                answer.addAll(ans);
                            }

                            long t2 = System.nanoTime();
                            long diff = t2 - t1;
                            joinTimeAcc.add(diff * Math.pow(10.0, -9.0));
                        }
                        return answer;
                    }
                }).filter(set -> set.isEmpty() ? false : true);
        List<Integer> collectedResult = resultSetRDD.collect().stream().flatMap(set -> set.stream()).distinct().collect(toList());

        for (Integer t : collectedResult) {
            System.out.println(t);
        }
        joinTimeAcc.setValue(joinTimeAcc.value());
        System.out.println("resultSetSize:" + collectedResult.size());
        System.out.println("joinTime(sec):" + joinTimeAcc.value());


    }
}
