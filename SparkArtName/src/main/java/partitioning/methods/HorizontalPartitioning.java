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
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.LongAccumulator;
import partitioners.IntegerPartitioner;
import scala.Tuple2;
import trie.Query;
import trie.Trie;
import utilities.CSVRecord;
import utilities.HorizontalAnswer;
import utilities.Parallelism;
import utilities.Trajectory;

import java.util.*;

/**
 * Created by giannis on 27/12/18.
 */
public class HorizontalPartitioning {


    public static void main(String[] args) {
        int horizontalPartitionSize = Integer.parseInt(args[0]);

        String appName = HorizontalPartitioning.class.getSimpleName() + horizontalPartitionSize;
//        String fileName = "file:////mnt/hgfs/VM_SHARED/trajDatasets/onesix.csv";
//        String queryFile = "file:////mnt/hgfs/VM_SHARED/trajDatasets/onesix.csv";
//        String fileName = "file:////mnt/hgfs/VM_SHARED/trajDatasets/half.csv";

//        String fileName = "file:////mnt/hgfs/VM_SHARED/trajDatasets/octant.csv";
//        String fileName = "file:///mnt/hgfs/VM_SHARED/samplePort.csv";
//        String queryFile = "file:///mnt/hgfs/VM_SHARED/samplePort.csv";
//        String fileName= "hdfs:////concatTrajectoryDataset.csv";
//        String fileName = "hdfs:////85TD.csv";
//        String fileName = "hdfs:////half.csv";
//        String fileName = "hdfs:////onesix.csv";
//        String fileName= "hdfs:////synthBigDataset.csv";
//        String fileName = "hdfs:////octant.csv";
//        String fileName = "hdfs:////65PC.csv";
//        String queryFile = "file:///mnt/hgfs/VM_SHARED/trajDatasets/queryRecords.csv";
//        String queryFile = "hdfs:////queryRecordsOnesix.csv";
        String fileName= "hdfs:////synth5GBDataset.csv";
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
                .flatMapValues(new HCSVRecToTrajME(horizontalPartitionSize)).
                        mapToPair(new PairFunction<Tuple2<Integer, Trajectory>, Integer, Trajectory>() {
                            @Override
                            public Tuple2<Integer, Trajectory> call(Tuple2<Integer, Trajectory> trajectoryTuple2) throws Exception {
                                Integer horizontalID = trajectoryTuple2._2().getHorizontalID();
                                return new Tuple2<>(horizontalID, trajectoryTuple2._2());
                            }
                        }).filter(new ReduceNofTrajectories());


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
        JavaPairRDD<Integer, Trie> partitionedTries = trieRDD.partitionBy(new IntegerPartitioner()).persist(StorageLevel.MEMORY_ONLY());
//        JavaPairRDD<Integer, Trie> partitionedTries = trieRDD.repartitionAndSortWithinPartitions(new IntegerPartitioner(horizontalPartitionSize)).persist(StorageLevel.MEMORY_ONLY());
//        JavaPairRDD<Integer, Trie> partitionedTries = trieRDD.repartition(99).persist(StorageLevel.MEMORY_ONLY());
        System.out.println("nofTries:" + partitionedTries.count());

        JavaPairRDD<Integer, Query> partitionedQueries = queries.partitionBy(new IntegerPartitioner()).persist(StorageLevel.MEMORY_ONLY());
//        JavaPairRDD<Integer, Query> partitionedQueries = queries.repartitionAndSortWithinPartitions(new IntegerPartitioner(horizontalPartitionSize)).persist(StorageLevel.MEMORY_ONLY());
//        JavaPairRDD<Integer, Query> partitionedQueries = queries.repartition(99).persist(StorageLevel.MEMORY_ONLY());
        partitionedQueries.count();
        System.out.println("TrieNofPartitions:"+partitionedTries.rdd().partitions().length);
        System.out.println("TriePartitioner:"+partitionedTries.rdd().partitioner());

        //        partitionedTries.repartitionAndSortWithinPartitions(new IntegerPartitioner(numberOfSlices));

//        System.out.println("nofTries::"+partitionedTries.keys().collect().size());
//        Stats.nofTriesInPartitions(partitionedTries);



        Broadcast<List<Tuple2<Integer, Query>>> partBroadQueries=sc.broadcast(queries.collect());





        JavaPairRDD<Integer, Integer> resultSet =
                partitionedTries.flatMap(new FlatMapFunction<Tuple2<Integer,Trie>, HorizontalAnswer>() {
            @Override
            public Iterator<HorizontalAnswer> call(Tuple2<Integer, Trie> v1) throws Exception {
                List<Tuple2<Integer, Query>> localQueries=partBroadQueries.value();
                TreeMap<Integer, HorizontalAnswer> mapOfAnswers=new TreeMap<>();

                for (Tuple2<Integer, Query> queryEntry:localQueries) {
                    HorizontalAnswer horAnswer=mapOfAnswers.get(queryEntry._2().getQueryID());

                    if (v1._2().getHorizontalTrieID()==queryEntry._2().getHorizontalPartition()) {
                        if (horAnswer==null){
                            horAnswer=new HorizontalAnswer(queryEntry._2().getQueryID());
                            mapOfAnswers.put(queryEntry._2().getQueryID(), horAnswer);
                        }
                        Set<Integer> ans=v1._2().queryIndex(queryEntry._2());
                        horAnswer.addTrajIDs(ans);
                    }
                }
             return    mapOfAnswers.values().iterator();
            }
        }).
                groupBy(ha -> ha.getQueryID()).flatMapValues(new Function<Iterable<HorizontalAnswer>, Iterable<Integer>>() {
            @Override
            public Iterable<Integer> call(Iterable<HorizontalAnswer> v1) throws Exception {

                TreeSet<Integer> intersection=new TreeSet<>();

                for (HorizontalAnswer ha:v1) {

                intersection.addAll(ha.getAnswer());
                }
                return intersection;
            }
        });








        List<Tuple2<Integer, Integer>> resultCollection = resultSet.collect();
        for (Tuple2<Integer, Integer> qID_trajID : resultCollection) {
            System.out.println(qID_trajID);
        }
        System.out.println("resultCollection.size():"+resultCollection.size());
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