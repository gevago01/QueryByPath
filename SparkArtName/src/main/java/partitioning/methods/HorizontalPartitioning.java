package partitioning.methods;

import com.google.common.collect.Lists;
import filtering.ReduceNofTrajectories;
import key.selectors.CSVTrajIDSelector;
import key.selectors.QueryIDSelector;
import map.functions.CSVRecordToTrajectory;
import map.functions.HCSVRecToTrajME;
import map.functions.LineToCSVRec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.LongAccumulator;
import partitioners.IntegerPartitioner;
import scala.Tuple2;
import trie.Query;
import trie.Trie;
import utilities.CSVRecord;
import utilities.Parallelism;
import utilities.Stats;
import utilities.Trajectory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.*;

/**
 * Created by giannis on 27/12/18.
 */
public class HorizontalPartitioning {


    public static void main(String[] args) {
        int horizontalPartitionSize = Integer.parseInt(args[0]);

        String appName = HorizontalPartitioning.class.getSimpleName() + horizontalPartitionSize;
//        String fileName = "file:////mnt/hgfs/VM_SHARED/trajDatasets/onesix.csv";
//        String fileName = "file:////mnt/hgfs/VM_SHARED/trajDatasets/half.csv";
//        String fileName= "hdfs:////synth5GBDataset.csv";
//        String fileName = "file:////mnt/hgfs/VM_SHARED/trajDatasets/octant.csv";
//        String fileName = "file:///mnt/hgfs/VM_SHARED/samplePort.csv";
        String fileName= "hdfs:////concatTrajectoryDataset.csv";
//        String fileName = "hdfs:////85TD.csv";
//        String fileName = "hdfs:////half.csv";
//        String fileName = "hdfs:////onesix.csv";
//        String fileName= "hdfs:////synthBigDataset.csv";
//        String fileName = "hdfs:////octant.csv";
//        String fileName = "hdfs:////65PC.csv";
//        String queryFile = "file:///mnt/hgfs/VM_SHARED/trajDatasets/queryRecords.csv";
//        String queryFile = "hdfs:////queryRecordsOnesix.csv";
        String queryFile = "hdfs:////queryRecords.csv";
//        SparkConf conf = new SparkConf().setAppName(appName)
//                .setMaster("local[*]")
//                .set("spark.executor.instances", "" + Parallelism.PARALLELISM);
        SparkConf conf = new SparkConf().setAppName(appName);


        JavaSparkContext sc = new JavaSparkContext(conf);
        LongAccumulator nofQueries = sc.sc().longAccumulator("NofQueries");
        DoubleAccumulator joinTimeAcc = sc.sc().doubleAccumulator("joinTimeAcc");
        JavaRDD<CSVRecord> records = sc.textFile(fileName).map(new LineToCSVRec());

        JavaPairRDD<Integer, Iterable<CSVRecord>> recordsCached = records.groupBy(new CSVTrajIDSelector()).cache();

        JavaPairRDD<Integer, Trajectory> trajectoryDataset = recordsCached
                .filter(new Function<Tuple2<Integer, Iterable<CSVRecord>>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<Integer, Iterable<CSVRecord>> v1) throws Exception {
                        return v1._2().spliterator().getExactSizeIfKnown() > ReduceNofTrajectories.MAX_TRAJECTORY_SIZE ? false : true;
                    }
                })
                .flatMapValues(new HCSVRecToTrajME(horizontalPartitionSize)).
                        mapToPair(new PairFunction<Tuple2<Integer, Trajectory>, Integer, Trajectory>() {
                            @Override
                            public Tuple2<Integer, Trajectory> call(Tuple2<Integer, Trajectory> trajectoryTuple2) throws Exception {
                                Integer horizontalID = trajectoryTuple2._2().getHorizontalID();
                                return new Tuple2<>(horizontalID, trajectoryTuple2._2());
                            }
                        });


        JavaPairRDD<Integer, Trie> trieRDD = trajectoryDataset.groupByKey().flatMap(new FlatMapFunction<Tuple2<Integer, Iterable<Trajectory>>, Trie>() {
            @Override
            public Iterator<Trie> call(Tuple2<Integer, Iterable<Trajectory>> integerIterableTuple2) throws Exception {
                List<Trie> trieList = new ArrayList<>();
                Iterable<Trajectory> trajectories = integerIterableTuple2._2();
                int horizontalID = integerIterableTuple2._1();
                Trie trie = new Trie();
                trie.setHorizontalTrieID(horizontalID);


                for (Trajectory traj : trajectories) {
                    trie.insertTrajectory2(traj);
                }
                trieList.add(trie);
                return trieList.iterator();
            }
        }).mapToPair(new PairFunction<Trie, Integer, Trie>() {
            @Override
            public Tuple2<Integer, Trie> call(Trie trie) throws Exception {
                return new Tuple2<>(trie.getHorizontalTrieID(), trie);
            }
        });

//        trieRDD.saveAsObjectFile(fileName+".o");


        JavaPairRDD<Integer, CSVRecord> queryRecords = sc.textFile(queryFile).map(new LineToCSVRec()).mapToPair(csvRec -> new Tuple2<>(csvRec.getTrajID(), csvRec));

//        trieRDD.checkpoint();

        JavaPairRDD<Integer, Query> queries =
                queryRecords.groupByKey().mapValues(new CSVRecordToTrajectory()).flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, Trajectory>, Integer, Query>() {
                    @Override
                    public Iterator<Tuple2<Integer, Query>> call(Tuple2<Integer, Trajectory> trajectoryTuple2) throws Exception {
                        List<List<Integer>> roadSegments = Lists.partition(trajectoryTuple2._2().roadSegments, horizontalPartitionSize);
                        List<List<Long>> timestamps = Lists.partition(trajectoryTuple2._2().timestamps, horizontalPartitionSize);

                        List<Tuple2<Integer, Query>> queryList = new ArrayList<>();

                        int horizontalID = 0;
                        for (int i = 0; i < roadSegments.size(); i++) {
                            List<Integer> subSegments = roadSegments.get(i);
                            List<Long> subTimestamps = timestamps.get(i);

                            Query query = new Query(subTimestamps.get(0), subTimestamps.get(subTimestamps.size() - 1), subSegments);
                            query.setQueryID(trajectoryTuple2._2().getTrajectoryID());
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
//        Stats.nofQueriesInEachTimeSlice(queries);

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
//        JavaPairRDD<Integer, Trie> partitionedTries = trieRDD.partitionBy(new IntegerPartitioner(horizontalPartitionSize)).persist(StorageLevel.MEMORY_ONLY());
//        partitionedTries.count();
//
//        JavaPairRDD<Integer, Query> partitionedQueries = queries.partitionBy(new IntegerPartitioner(horizontalPartitionSize)).persist(StorageLevel.MEMORY_ONLY());
//        partitionedQueries.count();


//        System.out.println("nofTries::"+partitionedTries.keys().collect().size());
//        Stats.nofTriesInPartitions(partitionedTries);

        trieRDD.count();
        queries.count();

        JavaPairRDD<Integer, Integer> resultSet =
                trieRDD.join(queries).mapValues(new Function<Tuple2<Trie, Query>, Tuple2<Integer, Set<Integer>>>() {
                    @Override
                    public Tuple2<Integer, Set<Integer>> call(Tuple2<Trie, Query> v1) throws Exception {
                        long t1 = System.nanoTime();
                        Tuple2<Integer, Set<Integer>> ans = new Tuple2<>(v1._2().getQueryID(), v1._1().queryIndex(v1._2()));
                        long t2 = System.nanoTime();
                        long diff = t2 - t1;
                        System.out.println("queryIndexTime:"+diff);
                        joinTimeAcc.add(diff * Math.pow(10.0, -9.0));
                        ;
                        return ans;
                    }
                }).values().groupBy(new QueryIDSelector()).flatMapValues(new Function<Iterable<Tuple2<Integer, Set<Integer>>>, Iterable<Integer>>() {
                    @Override
                    public Iterable<Integer> call(Iterable<Tuple2<Integer, Set<Integer>>> v1) throws Exception {
                        Set<Integer> answerSet = new TreeSet<>();

                        for (Tuple2<Integer, Set<Integer>> t : v1) {
                            for (Integer trajID : t._2()) {
                                answerSet.add(trajID);
                            }
                        }
                        return answerSet;
                    }
                });


        List<Tuple2<Integer, Integer>> resultCollection = resultSet.collect();
        for (Tuple2<Integer, Integer> qID_trajID : resultCollection) {
            System.out.println(qID_trajID);
        }
        System.out.println("resultCollection.size():" + resultCollection.size());
        System.out.println("MaxTrajLength:" + HCSVRecToTrajME.max);
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
//    List<Tuple2<Long, Iterable<CSVRecord>>> allRecords = recordSample.collect();
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