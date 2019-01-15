package partitioning.methods;

import com.google.common.collect.Lists;
import filtering.ReduceNofTrajectories;
import key.selectors.CSVTrajIDSelector;
import key.selectors.QueryIDSelector;
import map.functions.CSVRecordToTrajectory;
import map.functions.HCSVRecToTrajME;
import map.functions.LineToCSVRec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.LongAccumulator;
import partitioners.IntegerPartitioner;
import scala.Tuple2;
import trie.Query;
import trie.Trie;
import utilities.CSVRecord;
import utilities.Parallelism;
import utilities.Stats;
import utilities.Trajectory;

import java.util.*;
import java.util.stream.StreamSupport;

/**
 * Created by giannis on 27/12/18.
 */
public class HorizontalPartitioning {


    public static void main(String[] args) {
        int horizontalPartitionSize = Integer.parseInt(args[0]);

        String appName = HorizontalPartitioning.class.getSimpleName() + horizontalPartitionSize;
//        String fileName = "file:////mnt/hgfs/VM_SHARED/trajDatasets/onesix.csv";
//        String fileName = "file:////mnt/hgfs/VM_SHARED/trajDatasets/half.csv";
//        String fileName = "file:////mnt/hgfs/VM_SHARED/trajDatasets/octant.csv";
//        String fileName = "file:///mnt/hgfs/VM_SHARED/samplePort.csv";
        String fileName= "hdfs:////concatTrajectoryDataset.csv";
//        String fileName = "hdfs:////85TD.csv";
//        String fileName = "hdfs:////half.csv";
//        String fileName = "hdfs:////onesix.csv";
//        String fileName = "hdfs:////octant.csv";
//        String fileName = "hdfs:////65PC.csv";
//        SparkConf conf = new SparkConf().setAppName(appName)
//                .setMaster("local[*]")
//                .set("spark.executor.instances", "" + Parallelism.PARALLELISM)
//                .set("spark.executor.cores", "" + Parallelism.EXECUTOR_CORES);
        SparkConf conf = new SparkConf().setAppName(appName)
                .set("spark.executor.instances", "" + Parallelism.PARALLELISM)
                .set("spark.executor.cores","" + Parallelism.EXECUTOR_CORES );//.set("spark.executor.heartbeatInterval","15s");


        JavaSparkContext sc = new JavaSparkContext(conf);
        LongAccumulator nofQueries = sc.sc().longAccumulator("NofQueries");
        JavaRDD<CSVRecord> records = sc.textFile(fileName).map(new LineToCSVRec());

        JavaPairRDD<Long, Iterable<CSVRecord>> recordsCached = records.groupBy(new CSVTrajIDSelector()).cache();
        JavaPairRDD<Integer, Trajectory> trajectoryDataset = recordsCached.
                flatMapValues(new HCSVRecToTrajME(horizontalPartitionSize)).
                mapToPair(new PairFunction<Tuple2<Long, Trajectory>, Integer, Trajectory>() {
                    @Override
                    public Tuple2<Integer, Trajectory> call(Tuple2<Long, Trajectory> trajectoryTuple2) throws Exception {
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




//        @Override
//        public Query call(Trajectory trajectory) throws Exception {
//            Query q = new Query(trajectory.getStartingTime(),trajectory.getEndingTime(),trajectory.roadSegments);
//            q.setHorizontalPartition(trajectory.getHorizontalID());
//            nofQueries.add(1l);
//            return q;
        //DO the queries without splitting, then split them. then redirect each query to the corresponding trie
        //then group by queryID and remove duplicates 0.0.0001f
        JavaPairRDD<Integer, Query> queries = recordsCached.filter(new Function<Tuple2<Long, Iterable<CSVRecord>>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Long, Iterable<CSVRecord>> v1) throws Exception {
                long count = StreamSupport.stream(v1._2().spliterator(),false).count();
                return  count < ReduceNofTrajectories.MAX_TRAJECTORY_SIZE ? true : false;
            }
        }). sample(false, 1).mapValues(new CSVRecordToTrajectory()).flatMapValues(new Function<Trajectory, Iterable<Query>>() {
            @Override
            public Iterable<Query> call(Trajectory trajectory) throws Exception {

                List<List<Long>> roadSegments = Lists.partition(trajectory.roadSegments, horizontalPartitionSize);
                List<List<Long>> timestamps = Lists.partition(trajectory.timestamps, horizontalPartitionSize);

                List<Query> queryList = new ArrayList<>();

                int horizontalID = 0;
                for (int i = 0; i < roadSegments.size(); i++) {
                    List<Long> subSegments = roadSegments.get(i);
                    List<Long> subTimestamps = timestamps.get(i);

                    Query query = new Query(subTimestamps.get(0), subTimestamps.get(subTimestamps.size() - 1), subSegments);
                    query.setQueryID(trajectory.getTrajectoryID());
                    query.setHorizontalPartition(horizontalID++);
                    queryList.add(query);

                }
                return queryList;
            }
        }).values().mapToPair(new PairFunction<Query, Integer, Query>() {
            @Override
            public Tuple2<Integer, Query> call(Query query) throws Exception {
                int horizontalID = query.getHorizontalPartition();
                return new Tuple2<>(horizontalID, query);
            }
        });



        List<Integer> lengthList=queries.mapToPair(new PairFunction<Tuple2<Integer, Query>, Long, Query>() {
            @Override
            public Tuple2<Long, Query> call(Tuple2<Integer, Query> integerQueryTuple2) throws Exception {
                return new Tuple2<>(integerQueryTuple2._2().getQueryID(),integerQueryTuple2._2());
            }
        }).groupByKey().mapValues(new Function<Iterable<Query>, Integer>() {
            @Override
            public Integer call(Iterable<Query> v1) throws Exception {
                int sum=0;
                for (Query q:v1) {
                    sum+=q.getPathSegments().size();
                }
                return sum;
            }
        }).values().collect();
        Stats.printStats(lengthList);



        System.out.println("nofQueries:" + queries.count());
        JavaPairRDD<Integer, Trie> partitionedTries = trieRDD.partitionBy(new IntegerPartitioner()).persist(StorageLevel.MEMORY_ONLY());
        JavaPairRDD<Integer, Query> partitionedQueries = queries.partitionBy(new IntegerPartitioner()).persist(StorageLevel.MEMORY_ONLY());


        Stats.nofTriesInPartitions(partitionedTries);


        JavaPairRDD<Long, Long> resultSet =
                partitionedTries.join(partitionedQueries).mapValues(new Function<Tuple2<Trie, Query>, Tuple2<Long, Set<Long>>>() {
                    @Override
                    public Tuple2<Long, Set<Long>> call(Tuple2<Trie, Query> v1) throws Exception {
                        return new Tuple2<>(v1._2().getQueryID(), v1._1().queryIndex(v1._2()));
                    }
                }).values().groupBy(new QueryIDSelector()).flatMapValues(new Function<Iterable<Tuple2<Long, Set<Long>>>, Iterable<Long>>() {
                    @Override
                    public Iterable<Long> call(Iterable<Tuple2<Long, Set<Long>>> v1) throws Exception {
                        Set<Long> answerSet = new TreeSet<>();

                        for (Tuple2<Long, Set<Long>> t : v1) {
                            for (Long trajID : t._2()) {
                                answerSet.add(trajID);
                            }
                        }
                        return answerSet;
                    }
                });


        List<Tuple2<Long, Long>> resultCollection = resultSet.collect();

        for (Tuple2<Long, Long> qID_trajID : resultCollection) {
            System.out.println(qID_trajID);
        }

        System.out.println("resultCollection.size():" + resultCollection.size());
        System.out.println("MaxTrajLength:"+HCSVRecToTrajME.max);
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