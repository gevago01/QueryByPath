package partitioning.methods;

import com.google.common.collect.Iterables;
import comparators.IntegerComparator;
import comparators.LongComparator;
import key.selectors.CSVTrajIDSelector;
import map.functions.CSVRecordToTrajectory;
import map.functions.HCSVRecToTrajME2;
import map.functions.LineToCSVRec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.LongAccumulator;
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
        int nofPartitions = Integer.parseInt(args[0]);

        String appName = HorizontalPartitioning.class.getSimpleName() + nofPartitions;
//        String fileName = "file:////mnt/hgfs/VM_SHARED/trajDatasets/onesix.csv";
//        String queryFile = "file:////mnt/hgfs/VM_SHARED/trajDatasets/onesix.csv";
//        String fileName = "file:///home/giannis/IdeaProjects/SparkTrajectories/SparkArtName/timeSkewedDataset.csv";
//        String fileName = "file:////mnt/hgfs/VM_SHARED/trajDatasets/half.csv";

//        String fileName = "file:////mnt/hgfs/VM_SHARED/trajDatasets/octant.csv";
        String fileName = "file:////mnt/hgfs/VM_SHARED/trajDatasets/concatTrajectoryDataset.csv";
//        String fileName = "file:///mnt/hgfs/VM_SHARED/samplePort.csv";
        String queryFile = "file:///mnt/hgfs/VM_SHARED/samplePort.csv";
//        String fileName = "hdfs:////85TD.csv";
//        String fileName = "hdfs:////half.csv";
//        String fileName = "hdfs:////onesix.csv";
//        String fileName= "hdfs:////synthBigDataset.csv";
//        String fileName = "hdfs:////octant.csv";
//        String fileName = "hdfs:////65PC.csv";
//        String queryFile = "file:///mnt/hgfs/VM_SHARED/trajDatasets/queryRecords.csv";
//        String queryFile = "hdfs:////queryRecordsOnesix.csv";
//        String fileName = "hdfs:////synth5GBDataset.csv";
//        String queryFile = "hdfs:////queryRecords.csv";
//        String fileName = "hdfs:////concatTrajectoryDataset.csv";
//        String queryFile = "hdfs:////200KqueryRecords.csv";
        SparkConf conf = new SparkConf().setAppName(appName)
                .setMaster("local[*]")
                .set("spark.executor.instances", "" + Parallelism.PARALLELISM)
                .set("spark.driver.maxResultSize", "2g");
//        SparkConf conf = new SparkConf().setAppName(appName);


        JavaSparkContext sc = new JavaSparkContext(conf);
        LongAccumulator nofQueries = sc.sc().longAccumulator("NofQueries");
        DoubleAccumulator joinTimeAcc = sc.sc().doubleAccumulator("joinTimeAcc");
        JavaRDD<CSVRecord> records = sc.textFile(fileName).map(new LineToCSVRec());


        JavaPairRDD<Integer, Iterable<CSVRecord>> recordsCached = records.groupBy(new CSVTrajIDSelector()).cache();

        TrajectorySynthesizerME ts=new TrajectorySynthesizerME(records);


        try {
            ts.timeSkewedDataset();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.exit(1);

        JavaRDD<Integer> trajLengths = recordsCached.mapValues(it -> Iterables.size(it)).values();

        int maxTrajLength = trajLengths.max(new IntegerComparator());

        System.out.println("maxtrajlength:" + maxTrajLength);

        List<Long> intervals = Intervals.getIntervals(nofPartitions, 0, maxTrajLength);

        System.out.println("intervals.size():" + intervals.size());
        assert (intervals.size() == nofPartitions);

        JavaPairRDD<Integer, Trajectory> trajectoryDataset = recordsCached
                .flatMapValues(new HCSVRecToTrajME2(intervals)).
//                .flatMapValues(new HCSVRecToTrajME(horizontalPartitionSize)).
        mapToPair(new PairFunction<Tuple2<Integer, Trajectory>, Integer, Trajectory>() {
    @Override
    public Tuple2<Integer, Trajectory> call(Tuple2<Integer, Trajectory> trajectoryTuple2) throws Exception {
        Integer horizontalID = trajectoryTuple2._2().getHorizontalID();
        return new Tuple2<>(horizontalID, trajectoryTuple2._2());
    }
});//.filter(new ReduceNofTrajectories());
        trajectoryDataset.count();


//        trajectoryDataset.groupByKey().keys().foreach(new VoidFunction<Integer>() {
//            @Override
//            public void call(Integer integer) throws Exception {
//                System.out.println("tt::"+integer);
//            }
//        });

        List<Integer> partitions = IntStream.range(0, nofPartitions - 1).boxed().collect(toList());
        JavaRDD<Integer> partitionsRDD = sc.parallelize(partitions);
        JavaPairRDD<Integer, Trie> emptyTrieRDD = partitionsRDD.mapToPair(new PairFunction<Integer, Integer, Trie>() {
            @Override
            public Tuple2<Integer, Trie> call(Integer horizontalTrieID) throws Exception {
                Trie t = new Trie();
                t.setHorizontalTrieID(horizontalTrieID);
                return new Tuple2<>(horizontalTrieID, t);
            }
        });

        trajectoryDataset.keys().distinct().foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println("tt:" + integer);
            }
        });


        JavaRDD<Integer> setDifference = emptyTrieRDD.keys().subtract(trajectoryDataset.keys().distinct());


//        if (setDifference.count() > 0) {
//            System.err.println("setDifference is not empty");
//            System.exit(1);
//        }
//        System.exit(1);

        JavaPairRDD<Integer, Trie> trieRDD =
                emptyTrieRDD.groupWith(trajectoryDataset).mapValues(new Function<Tuple2<Iterable<Trie>, Iterable<Trajectory>>, Trie>() {
                    @Override
                    public Trie call(Tuple2<Iterable<Trie>, Iterable<Trajectory>> v1) throws Exception {
                        Trie trie = null;
                        try {
                            trie = v1._1().iterator().next();
                        } catch (NoSuchElementException e) {
//                            System.exit(1);
                        }

                        for (Trajectory trajectory : v1._2()) {
                            trie.insertTrajectory2(trajectory);
                        }
                        return trie;
                    }
                });


        JavaPairRDD<Integer, CSVRecord> queryRecords = sc.textFile(queryFile).map(new LineToCSVRec()).mapToPair(csvRec -> new Tuple2<>(csvRec.getTrajID(), csvRec));

        trieRDD.count();

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

        JavaPairRDD<Integer, Query> queries = queryRecords.groupByKey().mapValues(new CSVRecordToTrajectory()).flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, Trajectory>, Integer, Query>() {
            @Override
            public Iterator<Tuple2<Integer, Query>> call(Tuple2<Integer, Trajectory> hid_trajectory) throws Exception {
                int trajectoryLength = hid_trajectory._2().roadSegments.size();

//                HCSVRecToTrajME2.getIndexPositions(trajectoryLength,intervals);
                int until;
                for (until = 0; until < intervals.size() - 1; until++) {
                    if (trajectoryLength >= intervals.get(until) && trajectoryLength <= intervals.get(until + 1)) {
                        break;
                    }
                }

                List<List<Integer>> subRoadSegments = new ArrayList<>();
                List<List<Long>> subTimestamps = new ArrayList<>();
//


                for (int i = 0; i < until; i++) {
                    subRoadSegments.add(hid_trajectory._2.roadSegments.subList(intervals.get(i).intValue(), intervals.get(i + 1).intValue()));
                    subTimestamps.add(hid_trajectory._2.timestamps.subList(intervals.get(i).intValue(), intervals.get(i + 1).intValue()));
                }

                subRoadSegments.add(hid_trajectory._2.roadSegments.subList(intervals.get(until).intValue(), hid_trajectory._2.roadSegments.size()));
                subTimestamps.add(hid_trajectory._2.timestamps.subList(intervals.get(until).intValue(), hid_trajectory._2.roadSegments.size()));


//        if (allSubTrajs.size()>10) {
//            System.out.println("allSubTrajs.size():" + allSubTrajs.size());
//
//        }
                List<Tuple2<Integer, Query>> queryList = new ArrayList<>();
//
                int horizontalID = 0;
                for (int i = 0; i < subRoadSegments.size(); i++) {
                    List<Integer> subRS = subRoadSegments.get(i);
                    List<Long> subTS = subTimestamps.get(i);

                    Query query = new Query(subTS.get(0), subTS.get(subTS.size() - 1), subRS);
                    query.setQueryID(hid_trajectory._2().getTrajectoryID());
                    query.setHorizontalPartition(horizontalID++);
                    queryList.add(new Tuple2<>(query.getHorizontalPartition(), query));

                }
                return queryList.iterator();
            }
        });


        List<Integer> lengthList = queries.mapToPair(new PairFunction<Tuple2<Integer, Query>, Integer, Query>() {
            @Override
            public Tuple2<Integer, Query> call(Tuple2<Integer, Query> integerQueryTuple2) throws Exception {
                return new Tuple2<>(integerQueryTuple2._2().getQueryID(), integerQueryTuple2._2());
            }
        }).groupByKey().mapValues(new Function<Iterable<Query>, Integer>() {
            @Override
            public Integer call(Iterable<Query> v1) throws Exception {
                int sum = 0;
                for (Query q : v1) {
                    sum += q.getPathSegments().size();
                }
                return sum;
            }
        }).values().collect();
//        Stats.printStats(lengthList);
        Stats.nofQueriesOnEachNode(queries, PartitioningMethods.HORIZONTAL);
        Stats.nofTrajsOnEachNode(trajectoryDataset, PartitioningMethods.HORIZONTAL);

        JavaRDD<Integer> queryKeys = queries.mapToPair(new PairFunction<Tuple2<Integer, Query>, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Query> t) throws Exception {
                return new Tuple2<Integer, Integer>(t._2().getQueryID(), 1);
            }
        }).groupByKey().keys();
//                queries.groupBy(new Function<Tuple2<Integer, Query>, Integer>() {
//            @Override
//            public Integer call(Tuple2<Integer, Query> v1) throws Exception {
//                return v1._2().getTimeSlice();
//            }
//        }).


        System.out.println("nofQueries:" + queryKeys.count());


        System.out.println("TrieNofPartitions:" + trieRDD.rdd().partitions().length);
        System.out.println("TriePartitioner:" + trieRDD.rdd().partitioner());

        //        partitionedTries.repartitionAndSortWithinPartitions(new IntegerPartitioner(numberOfSlices));

//        System.out.println("nofTries::"+partitionedTries.keys().collect().size());
//        Stats.nofTriesInPartitions(partitionedTries);


        Broadcast<List<Tuple2<Integer, Query>>> partBroadQueries = sc.broadcast(queries.collect());

        trieRDD.count();

        JavaPairRDD<Integer, Integer> resultSet =
                trieRDD.flatMap(new FlatMapFunction<Tuple2<Integer, Trie>, HorizontalAnswer>() {
                    TreeMap<Integer, HorizontalAnswer> mapOfAnswers = new TreeMap<>();

                    @Override
                    public Iterator<HorizontalAnswer> call(Tuple2<Integer, Trie> v1) throws Exception {
                        List<Tuple2<Integer, Query>> localQueries = partBroadQueries.value();


                        for (Tuple2<Integer, Query> queryEntry : localQueries) {


                            if (v1._2().getHorizontalTrieID() == queryEntry._2().getHorizontalPartition()) {
                                HorizontalAnswer horAnswer = mapOfAnswers.get(queryEntry._2().getQueryID());
                                if (horAnswer == null) {
                                    horAnswer = new HorizontalAnswer(queryEntry._2().getQueryID());
                                    mapOfAnswers.put(queryEntry._2().getQueryID(), horAnswer);
                                }
                                long t1 = System.nanoTime();
                                Set<Integer> ans = v1._2().queryIndex(queryEntry._2());
                                long t2 = System.nanoTime();
                                long diff = t2 - t1;
                                joinTimeAcc.add(diff * Math.pow(10.0, -9.0));
                                horAnswer.addTrajIDs(ans);
                            }
                        }

                        return mapOfAnswers == null ? null : mapOfAnswers.values().iterator();
                    }
                }).filter(t -> t == null ? false : true).groupBy(ha -> ha.getQueryID()).flatMapValues(new Function<Iterable<HorizontalAnswer>, Iterable<Integer>>() {
                    @Override
                    public Iterable<Integer> call(Iterable<HorizontalAnswer> v1) throws Exception {

                        TreeSet<Integer> intersection = new TreeSet<>();

                        for (HorizontalAnswer ha : v1) {

                            intersection.addAll(ha.getAnswer());
                        }
                        return intersection;
                    }
                });


        List<Tuple2<Integer, Integer>> resultCollection = resultSet.collect();
        Set<Integer> ss = new TreeSet<>();
        for (Tuple2<Integer, Integer> qID_trajID : resultCollection) {
            System.out.println(qID_trajID);
            ss.add(qID_trajID._2);
        }
        System.out.println("resultCollection.size():" + ss.size());
    }


}

//BROADCASTING APPROACH
//        JavaPairRDD<Integer, Query> broadcastedQueries =
//        queries.col
//        Broadcast<List<Tuple2<Integer,Query>>> broadcastedQueries=sc.broadcast(queries.collect());
//        JavaRDD resultSet=trieRDD.map(new Function<Tuple2<Integer,Trie>, Set<Long>>() {
//            @Override
//            public Set<Long> call(Tuple2<Integer, Trie> v1) throws Exception {
//                List<Tuple2<Integer,Query>> bqlist=broadcastedQueries.getValue();
//
//                Set<Long> answer=new TreeSet<>();
//                for (Tuple2<Integer,Query>  query:bqlist) {
//                    if (query._1()!=v1._1()){
//                        continue;
//                    }
//                    answer.addAll(v1._2().queryIndex(query._2()));
//                }
//                return answer;
//            }
//        });


// writing queries to file
//JavaPairRDD<Integer, Iterable<CSVRecord>> recordSample=recordsCached.sample(false,0.1);
//    List<Tuple2<Integer, Iterable<CSVRecord>>> allRecords = recordSample.collect();
//
//        System.out.println("recordSample.count():" + recordSample.groupByKey().keys().count());
//                try {
//                BufferedWriter bf = new BufferedWriter(new FileWriter(new File("queryRecordsOnesix.csv")));
//
//                for (Tuple2<Long, Iterable<CSVRecord>> tuple : allRecords) {
//
//
//        Iterable<CSVRecord> trajectory = tuple._2();
//
//
//        for (CSVRecord csvRec : trajectory) {
//
//        bf.write(csvRec.getTrajID() + ", " + csvRec.getTimestamp() + ", " + csvRec.getRoadSegment() + "\n");
//
//        }
//        }
//
//        bf.close();
//        } catch (java.io.IOException e) {
//        e.printStackTrace();
//        } finally {
//        }
//