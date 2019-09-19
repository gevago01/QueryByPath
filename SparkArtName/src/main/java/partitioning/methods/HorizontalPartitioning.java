package partitioning.methods;

import com.google.common.collect.Iterables;
import comparators.IntegerComparator;
import comparators.LongComparator;
import map.functions.AddTrajToTrie;
import map.functions.CSVRecToTrajME;
import map.functions.LineToCSVRec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.LongAccumulator;
import projections.ProjectRoadSegments;
import projections.ProjectTimestamps;
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
        if (args.length > 1) {
            dataHdfsName = args[0];
            queryHdfsName = args[1];
            bucketCapacity = Integer.parseInt(args[2]);
        }
        String fileName = "hdfs:////"+dataHdfsName;
        String queryFile = "hdfs:////"+queryHdfsName;


//        String fileName = "file:////mnt/hgfs/VM_SHARED/trajDatasets/onesix.csv";
//        String queryFile = "file:////mnt/hgfs/VM_SHARED/trajDatasets/onesix.csv";
//        String queryFile = "file:////mnt/hgfs/VM_SHARED/trajDatasets/roadSegmentSkewedQueries";
//        String fileName = "file:////home/giannis/IdeaProjects/SparkTrajectories/SparkArtName/roadSegmentSkewedDataset.csv";
//        String queryFile = "file:////home/giannis/IdeaProjects/SparkTrajectories/SparkArtName/queryRecordsOnesix.csv";

//        String fileName = "file:////mnt/hgfs/VM_SHARED/trajDatasets/concatTrajectoryDataset.csv";
//        String queryFile = "file:///mnt/hgfs/VM_SHARED/samplePort.csv";
//        String fileName = "file:////mnt/hgfs/VM_SHARED/trajDatasets/lengthSkewedDataset/"+TrajectorySynthesizer.PROBABILITY+"pcLS.csv";
//        String fileName = "file:////mnt/hgfs/VM_SHARED/trajDatasets/timeSkewedDataSets/20pcTS.csv";
//        String fileName = "file:////mnt/hgfs/VM_SHARED/trajDatasets/roadSegmentSkewedDataset.csv";
//        String fileName = "file:////mnt/hgfs/VM_SHARED/trajDatasets/timeSkewedDataset";
//        String fileName = "file:///home/giannis/IdeaProjects/SparkTrajectories/SparkArtName/timeSkewedDataset.csv";
//        String fileName = "file:///home/giannis/IdeaProjects/SparkTrajectories/SparkArtName/roadSegmentSkewedDataset.csv";


//        SparkConf conf = new SparkConf()
//                .setMaster("local[*]")
//                .set("spark.driver.maxResultSize", "3g")
//                .set("spark.executor.instances", "" + Parallelism.PARALLELISM)
//                .setAppName(HorizontalPartitioning.class.getSimpleName());
        SparkConf conf = new SparkConf().setAppName(HorizontalPartitioning.class.getSimpleName()+bucketCapacity);


        JavaSparkContext sc = new JavaSparkContext(conf);
        DoubleAccumulator joinTimeAcc = sc.sc().doubleAccumulator("joinTimeAcc");
        JavaRDD<CSVRecord> records = sc.textFile(fileName).map(new LineToCSVRec());


        Integer skewnessFactor = 3;
        try {
            String skewness = dataHdfsName.substring(0, 2);
            skewnessFactor = Integer.parseInt(skewness);
        }
        catch (Exception e){

        }
        HybridConfiguration.configure(records, skewnessFactor);
        if (args.length!=3) {
            bucketCapacity = HybridConfiguration.getBucketCapacityLowerBound();
        }

//        int bucketCapacity = HybridConfiguration.getBucketCapacityLowerBound();
//        bucketCapacity = Integer.parseInt(args[2]);
//        int bucketCapacity = 1000;
//        String appName = HorizontalPartitioning.class.getSimpleName() + bucketCapacity;
//        conf.setAppName(appName);
//        System.out.println("appName:" + appName);



//        JavaPairRDD<Integer, Iterable<CSVRecord>> recordsCached = records.groupBy(csv -> csv.getTrajID()).cache();


//        QuerySynthesizer.synthesize(recordsCached);
//        System.exit(1);
//        TrajectorySynthesizer ts = new TrajectorySynthesizer(recordsCached, minTimestamp, maxTimestamp, maxTrajectoryID, minRS, maxRS);
//        try {
//            ts.timeSkewedDataset();
//            ts.lenSkewedDataset();
//            ts.roadSegmentSkewedDataset();
//            ts.lengthSkewedDataset();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        System.exit(1);
//        JavaRDD<Integer> trajLengths = recordsCached.mapValues(it -> Iterables.size(it)).values();
//        int maxTrajLength = trajLengths.max(new IntegerComparator());
//
//        System.out.println("maxtrajlength:" + maxTrajLength);


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
        if (bucketCapacity>nofTrajs){
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


        records.unpersist(true);
        JavaPairRDD<Integer, Trie> trieRDD = emptyTrieRDD.groupWith(trajectoryDataset).mapValues(new AddTrajToTrie());
        long noftries = trieRDD.count();
        trajectoryDataset.unpersist(true);
        System.out.println("nofTries:" + noftries);

        System.exit(1);

//        trieRDD.checkpoint();

//        JavaPairRDD<Integer, Query> queries =
//                queryRecords.groupByKey().mapValues(new CSVRecordToTrajectory()).flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, Trajectory>, Integer, Query>() {
//                    @Override
//                    public Iterator<Tuple2<Integer, Query>> call(Tuple2<Integer, Trajectory> trajectoryTuple2) throws Exception {
//                        List<List<Integer>> roadSegments = Lists.partition(trajectoryTuple2._2().roadSegments, horizontalPartitionSize);
//                        List<List<Long>> timestamps = Lists.partition(trajectoryTuple2._2().timestamps, horizontalPartitionSize);
//
//                        List<Tuple2<Integer, Query>> queryList = new ArrayList<>();
//
//                        int horizontalID = 0;
//                        for (int i = 0; i < roadSegments.size(); i++) {
//                            List<Integer> subSegments = roadSegments.get(i);
//                            List<Long> subTimestamps = timestamps.get(i);
//
//                            Query query = new Query(subTimestamps.get(0), subTimestamps.get(subTimestamps.size() - 1), subSegments);
//                            query.setQueryID(trajectoryTuple2._2().getTrajectoryID());
//                            query.setHorizontalPartition(horizontalID++);
//                            queryList.add(new Tuple2<>(query.getHorizontalPartition(), query));
//
//                        }
//                        return queryList.iterator();
//                    }
//                });

//        JavaPairRDD<Integer, Query> queries = queryRecords.groupByKey().mapValues(new CSVRecordToTrajectory()).flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, Trajectory>, Integer, Query>() {
//            @Override
//            public Iterator<Tuple2<Integer, Query>> call(Tuple2<Integer, Trajectory> hid_trajectory) throws Exception {
//                int trajectoryLength = hid_trajectory._2().roadSegments.size();
//
////                HCSVRecToTrajME2.getIndexPositions(trajectoryLength,intervals);
//                int until;
//                for (until = 0; until < intervals.size() - 1; until++) {
//                    if (trajectoryLength >= intervals.get(until) && trajectoryLength <= intervals.get(until + 1)) {
//                        break;
//                    }
//                }
//
//                List<List<Integer>> subRoadSegments = new ArrayList<>();
//                List<List<Long>> subTimestamps = new ArrayList<>();
////
//
//
//                for (int i = 0; i < until; i++) {
//                    subRoadSegments.add(hid_trajectory._2.roadSegments.subList(intervals.get(i).intValue(), intervals.get(i + 1).intValue()));
//                    subTimestamps.add(hid_trajectory._2.timestamps.subList(intervals.get(i).intValue(), intervals.get(i + 1).intValue()));
//                }
//
//                subRoadSegments.add(hid_trajectory._2.roadSegments.subList(intervals.get(until).intValue(), hid_trajectory._2.roadSegments.size()));
//                subTimestamps.add(hid_trajectory._2.timestamps.subList(intervals.get(until).intValue(), hid_trajectory._2.roadSegments.size()));
//
//
////        if (allSubTrajs.size()>10) {
////            System.out.println("allSubTrajs.size():" + allSubTrajs.size());
////
////        }
//                List<Tuple2<Integer, Query>> queryList = new ArrayList<>();
////
//                int horizontalID = 0;
//                for (int i = 0; i < subRoadSegments.size(); i++) {
//                    List<Integer> subRS = subRoadSegments.get(i);
//                    List<Long> subTS = subTimestamps.get(i);
//
//                    Query query = new Query(subTS.get(0), subTS.get(subTS.size() - 1), subRS);
//                    query.setQueryID(hid_trajectory._2().getTrajectoryID());
//                    query.setPartitionID(horizontalID++);
//                    queryList.add(new Tuple2<>(query.getPartitionID(), query));
//
//                }
//                return queryList.iterator();
//            }
//        });


        JavaPairRDD<Integer, CSVRecord> queryRecords = sc.textFile(queryFile).map(new LineToCSVRec()).mapToPair(csvRec -> new Tuple2<>(csvRec.getTrajID(), csvRec));
        JavaPairRDD<Integer, Query> queries =
                queryRecords.groupByKey().mapValues(new CSVRecToTrajME()).map(t -> new Query(t._2())).mapToPair(q -> {

//                    q.setPartitionID(buckets.getOrDefault(q.getQueryID(),1));
                    return new Tuple2<>(q.getPartitionID(), q);
                });
//        Broadcast<Map<Integer, Query>> broadcastedQueries = sc.broadcast(queries.collectAsMap());
        System.out.println("nofQueries:" + queries.count());
//        Stats.nofQueriesInSlice(queries, PartitioningMethods.TIME_SLICING);



//        Stats.nofQueriesOnEachNode(queries, PartitioningMethods.TIME_SLICING);
//        Stats.nofQueriesInSlice(queries);
//        Stats.nofTriesInPartitions(trieRDD);
        Broadcast<List<Tuple2<Integer, Query>>> partBroadQueries = sc.broadcast(queries.collect());


//        List<Integer> lengthList = queries.mapToPair(new PairFunction<Tuple2<Integer, Query>, Integer, Query>() {
//            @Override
//            public Tuple2<Integer, Query> call(Tuple2<Integer, Query> integerQueryTuple2) throws Exception {
//                return new Tuple2<>(integerQueryTuple2._2().getQueryID(), integerQueryTuple2._2());
//            }
//        }).groupByKey().mapValues(new Function<Iterable<Query>, Integer>() {
//            @Override
//            public Integer call(Iterable<Query> v1) throws Exception {
//                int sum = 0;
//                for (Query q : v1) {
//                    sum += q.getPathSegments().size();
//                }
//                return sum;
//            }
//        }).values().collect();
//        Stats.printStats(lengthList);
//        Stats.nofQueriesOnEachNode(queries, PartitioningMethods.HORIZONTAL);
//        Stats.nofTrajsOnEachNode(trajectoryDataset, sc);
//        Stats.nofTrajsInEachSlice(trajectoryDataset, PartitioningMethods.HORIZONTAL);
//        Stats.nofQueriesInSlice(queries, PartitioningMethods.HORIZONTAL);

//        JavaRDD<Integer> queryKeys = queries.mapToPair(new PairFunction<Tuple2<Integer, Query>, Integer, Integer>() {
//            @Override
//            public Tuple2<Integer, Integer> call(Tuple2<Integer, Query> t) throws Exception {
//                return new Tuple2<Integer, Integer>(t._2().getQueryID(), 1);
//            }
//        }).groupByKey().keys();
//        System.out.println("nofQueries:" + queryKeys.count());
//                queries.groupBy(new Function<Tuple2<Integer, Query>, Integer>() {
//            @Override
//            public Integer call(Tuple2<Integer, Query> v1) throws Exception {
//                return v1._2().getPartitionID();
//            }
//        }).


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
//                                if (queryEntry._2().getPathSegments().size() >= v1._2().getMinTrajLength() && queryEntry._2().getPathSegments().size() <= v1._2().getMaxTrajLength()) {
                                    ans = v1._2().queryIndex(queryEntry._2());
//                                }
//                            }
                            long t2 = System.nanoTime();
                            if (ans!=null) {
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
