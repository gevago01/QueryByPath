package partitioning.methods;

import map.functions.AddTrajToTrie;
import map.functions.CSVRecToTrajME;
import map.functions.LineToCSVRec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.DoubleAccumulator;
import scala.Tuple2;
import trie.Query;
import trie.Trie;
import utilities.*;

import java.io.IOException;
import java.util.*;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

/**
 * Created by giannis on 27/12/18.
 */
public class HorizontalPartitioning {


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
//        String fileName = "hdfs:////" + dataHdfsName;
//        String queryFile = "hdfs:////" + queryHdfsName;
//        String fileName = "file:////mnt/hgfs/VM_SHARED/trajDatasets/sizeExps/600000objects_synthDataset.csv";
//        String fileName = "file:////mnt/hgfs/VM_SHARED/trajDatasets/concatTrajectoryDataset.csv";
//        String queryFile = "file:///mnt/hgfs/VM_SHARED/samplePort.csv";


        String fileName = "file:////mnt/hgfs/VM_SHARED/trajDatasets/onesix.csv";
        String queryFile = "file:////mnt/hgfs/VM_SHARED/trajDatasets/onesix.csv";
//        String queryFile = "file:////mnt/hgfs/VM_SHARED/trajDatasets/roadSegmentSkewedQueries";
//        String fileName = "file:////home/giannis/IdeaProjects/SparkTrajectories/SparkArtName/roadSegmentSkewedDataset.csv";
//        String queryFile = "file:////home/giannis/IdeaProjects/SparkTrajectories/SparkArtName/queryRecordsOnesix.csv";

//        String fileName = "file:////mnt/hgfs/VM_SHARED/trajDatasets/lengthSkewedDataset/"+TrajectorySynthesizer.PROBABILITY+"pcLS.csv";
//        String fileName = "file:////mnt/hgfs/VM_SHARED/trajDatasets/timeSkewedDataSets/20pcTS.csv";
//        String fileName = "file:////mnt/hgfs/VM_SHARED/trajDatasets/roadSegmentSkewedDataset.csv";
//        String fileName = "file:////mnt/hgfs/VM_SHARED/trajDatasets/timeSkewedDataset";
//        String fileName = "file:///home/giannis/IdeaProjects/SparkTrajectories/SparkArtName/timeSkewedDataset.csv";
//        String fileName = "file:///home/giannis/IdeaProjects/SparkTrajectories/SparkArtName/roadSegmentSkewedDataset.csv";


        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .set("spark.driver.maxResultSize", "4g")
                .set("spark.executor.instances", "" + Parallelism.PARALLELISM)
                .setAppName(HorizontalPartitioning.class.getSimpleName());
//        SparkConf conf = new SparkConf().setAppName(HorizontalPartitioning.class.getSimpleName() + bucketCapacity);
        JavaSparkContext sc = new JavaSparkContext(conf);

        DoubleAccumulator joinTimeAcc = sc.sc().doubleAccumulator("joinTimeAcc");
        JavaRDD<CSVRecord> records = sc.textFile(fileName).map(new LineToCSVRec());


        Integer skewnessFactor;
        try {
            String skewness = dataHdfsName.substring(0, 2);
            skewnessFactor = Integer.parseInt(skewness);
        } catch (Exception e) {
            skewnessFactor = 1;
        }
        HybridConfiguration.configure(records, skewnessFactor);
        if (args.length != 3) {
            bucketCapacity = HybridConfiguration.getBucketCapacityLowerBound();
        }



//        System.exit(1);
//        QuerySynthesizer.synthesize(records);
//        System.exit(1);
        TrajectorySynthesizer ts = new TrajectorySynthesizer(records);
//        try {
//            ts.timeSkewedDataset();
//            ts.lenSkewedDataset();
//            ts.roadSegmentSkewedDataset();
//            ts.lengthSkewedDataset();
//            ts.synthesize();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        System.exit(1);


        JavaPairRDD<Integer, Trajectory> trajectoryDataset = records.groupBy(csv -> csv.getTrajID()).mapValues(new CSVRecToTrajME());
//        trajectoryDataset.collect();
//        Tuple2<Integer, Integer> busiestRS=trajectoryDataset.mapToPair(new PairFunction<Tuple2<Integer,Trajectory>, Integer, Integer>() {
//            @Override
//            public Tuple2<Integer, Integer> call(Tuple2<Integer, Trajectory> t) throws Exception {
//                return new Tuple2<>(t._2().getStartingRS(),1);
//            }
//        }).reduceByKey((l1,l2) -> l1+l2).mapToPair(p -> new Tuple2<>(p._2(),p._1())).sortByKey(false). first();
//
//        System.out.println("freq:"+busiestRS._1()+", rid:"+busiestRS._2());
//
//        System.exit(1);
        int nofTrajs = (int) trajectoryDataset.count();
        if (bucketCapacity > nofTrajs) {
            System.err.println("bucketCapacity>nofTrajs");
            System.exit(1);
        }

        HashMap<Integer, Integer> buckets = Intervals.sliceHorizontalToBuckets2(trajectoryDataset, bucketCapacity);
        trajectoryDataset = trajectoryDataset.mapToPair(trajectory -> {
            Integer bucket = buckets.get(trajectory._2().getTrajectoryID());

            trajectory._2().setPartitionID(bucket);
            return new Tuple2<>(trajectory._2().getPartitionID(), trajectory._2());
        });


        System.out.println("nofTrajs:" + nofTrajs);


//        trajectoryDataset.groupByKey().keys().foreach(new VoidFunction<Integer>() {
//            @Override
//            public void call(Integer integer) throws Exception {
//                System.out.println("tt::"+integer);
//            }
//        });

        int nofBuckets = Math.toIntExact(buckets.values().stream().distinct().count());
        System.out.println("nofBuckets:" + nofBuckets);
        List<Integer> partitions = IntStream.range(0, nofBuckets).boxed().collect(toList());
        JavaRDD<Integer> partitionsRDD = sc.parallelize(partitions);

        JavaPairRDD<Integer, Trie> emptyTrieRDD = partitionsRDD.mapToPair(new PairFunction<Integer, Integer, Trie>() {
            @Override
            public Tuple2<Integer, Trie> call(Integer horizontalTrieID) throws Exception {
                Trie t = new Trie();
                t.setHorizontalTrieID(horizontalTrieID);
                return new Tuple2<>(horizontalTrieID, t);
            }
        });


        emptyTrieRDD.count();


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
//        Stats.nofQueriesInSlice(queries, PartitioningMethods.TIME_SLICING);


        Broadcast<List<Tuple2<Integer, Query>>> partBroadQueries = sc.broadcast(queries.collect());



        JavaRDD<HorizontalAnswer> resultSet =
                trieRDD.flatMap(new FlatMapFunction<Tuple2<Integer, Trie>, HorizontalAnswer>() {
                    TreeMap<Integer, HorizontalAnswer> mapOfAnswers = new TreeMap<>();

                    @Override
                    public Iterator<HorizontalAnswer> call(Tuple2<Integer, Trie> v1) throws Exception {
                        List<Tuple2<Integer, Query>> localQueries = partBroadQueries.value();


                        for (Tuple2<Integer, Query> queryEntry : localQueries) {


//                            if (v1._2().getHorizontalTrieID() == queryEntry._2().getPartitionID()) {
                            HorizontalAnswer horAnswer = mapOfAnswers.get(queryEntry._2().getQueryID());
                            if (horAnswer == null) {
                                horAnswer = new HorizontalAnswer(queryEntry._2().getQueryID());
                                mapOfAnswers.put(queryEntry._2().getQueryID(), horAnswer);
                            }
                            Set<Integer> ans = null;
                            long t1 = System.nanoTime();
//                            if (queryEntry._2().getStartingRoadSegment() >= v1._2().getMinStartingRS() && queryEntry._2().getStartingRoadSegment() <= v1._2().getMaxStartingRS()) {
                            if (queryEntry._2().getPathSegments().size() >= v1._2().getMinTrajLength() && queryEntry._2().getPathSegments().size() <= v1._2().getMaxTrajLength()) {
                                ans = v1._2().queryIndex(queryEntry._2());
                            }
//                            }
                            long t2 = System.nanoTime();
                            if (ans != null) {
                                horAnswer.addTrajIDs(ans);
                            }

                            long diff = t2 - t1;
                            joinTimeAcc.add(diff * Math.pow(10.0, -9.0));

//                            }
                        }

                        return mapOfAnswers == null ? null : mapOfAnswers.values().iterator();
                    }
                }).filter(t -> t == null ? false : true);
//                        .groupBy(ha -> ha.getQueryID()).flatMapValues(new Function<Iterable<HorizontalAnswer>, Iterable<Integer>>() {
//                    @Override
//                    public Iterable<Integer> call(Iterable<HorizontalAnswer> v1) throws Exception {
//
//                        TreeSet<Integer> intersection = new TreeSet<>();
//
//                        for (HorizontalAnswer ha : v1) {
//
//                            intersection.addAll(ha.getAnswer());
//                        }
//                        return intersection;
//                    }
//                });


        List<HorizontalAnswer> resultCollection = resultSet.collect();
        Set<Integer> ss = new TreeSet<>();
        for (HorizontalAnswer qID_trajID : resultCollection) {
//            System.out.println(qID_trajID);
            ss.addAll(qID_trajID.getAnswer());
        }
        for (int answer : ss) {
            System.out.println(answer);
        }
        System.out.println("resultCollection.size():" + ss.size());
    }


}
