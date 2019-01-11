package partitioning.methods;

import filtering.FilterEmptyAnswers;
import filtering.FilterNullQueries;
import key.selectors.CSVTrajIDSelector;
import map.functions.CSVRecToTrajME;
import map.functions.HCSVRecToTrajME;
import map.functions.LineToCSVRec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
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
import utilities.Trajectory;

import java.util.*;

/**
 * Created by giannis on 27/12/18.
 */
public class HorizontalPartitioning {


    public static void main(String[] args) {
        int horizontalPartitionSize = Integer.parseInt(args[0]);

        String fileName = "file:////mnt/hgfs/VM_SHARED/trajDatasets/onesix.csv";
//        String fileName = "file:////mnt/hgfs/VM_SHARED/trajDatasets/half.csv";
//        String fileName = "file:////mnt/hgfs/VM_SHARED/trajDatasets/octant.csv";
//        String fileName = "file:///mnt/hgfs/VM_SHARED/samplePort.csv";
//        String fileName= "hdfs:////concatTrajectoryDataset.csv";
//        String fileName = "hdfs:////85TD.csv";
//        String fileName = "hdfs:////half.csv";
//        String fileName = "hdfs:////onesix.csv";
//        String fileName = "hdfs:////octant.csv";
//        String fileName = "hdfs:////65PC.csv";
        SparkConf conf = new SparkConf().setAppName(HorizontalPartitioning.class.getSimpleName())
                .setMaster("local[*]")
                .set("spark.executor.instances", "" + Parallelism.PARALLELISM)
                .set("spark.executor.cores", "" + Parallelism.EXECUTOR_CORES);
//        SparkConf conf = new SparkConf().setAppName(HorizontalPartitioning.class.getSimpleName())
//                .set("spark.executor.instances", "" + Parallelism.PARALLELISM)
//                .set("spark.executor.cores","" + Parallelism.EXECUTOR_CORES );//.set("spark.executor.heartbeatInterval","15s");


        JavaSparkContext sc = new JavaSparkContext(conf);
        LongAccumulator nofQueries=sc.sc().longAccumulator("NofQueries");
        JavaRDD<CSVRecord> records = sc.textFile(fileName).map(new LineToCSVRec());

        JavaPairRDD<Long, Iterable<CSVRecord>> recordsCached=records.groupBy(new CSVTrajIDSelector()).cache();
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
                Integer horizontalID = integerIterableTuple2._1();
                Trie trie = new Trie();
                trie.setHorizontalTrieID(horizontalID);


                for (Trajectory traj : trajectories) {
                    trie.insertTrajectory2(traj.roadSegments, traj.trajectoryID, traj.getStartingTime(), traj.getEndingTime());
                }
//                System.out.println("nof Trajs in this group:" + counter + " with an average size of:" + horizontalPartitionSize * trie.getHorizontalTrieID());
                trieList.add(trie);
                return trieList.iterator();
            }
        }).mapToPair(new PairFunction<Trie, Integer, Trie>() {
            @Override
            public Tuple2<Integer, Trie> call(Trie trie) throws Exception {
                return new Tuple2<>(trie.getHorizontalTrieID(), trie);
            }
        });


        recordsCached.mapValues(new CSVRecToTrajME()).sample(false,400);
        JavaPairRDD<Integer, Query> queries =
                trajectoryDataset.values().map(new Function<Trajectory, Query>() {

                    private Random random = new Random();

                    private final static float PROBABILITY = 0.0002f;
//                    private final static float PROBABILITY = 1f;

                    private boolean getRandomBoolean(float p) {

                        return random.nextFloat() < p;
                    }

                    @Override
                    public Query call(Trajectory trajectory) throws Exception {
                        if (getRandomBoolean(PROBABILITY)) {
                            Query q = new Query(trajectory.getStartingTime(),trajectory.getEndingTime(),trajectory.roadSegments);
                            q.setHorizontalPartition(trajectory.getHorizontalID());
                            nofQueries.add(1l);
                            return q;
                        }
                        return null;
                    }
                }).filter(new FilterNullQueries()).mapToPair(new PairFunction<Query, Integer, Query>() {
                    @Override
                    public Tuple2<Integer, Query> call(Query query) throws Exception {
                        Integer horizontalID = query.getHorizontalPartition();
                        return new Tuple2<>(horizontalID, query);
                    }
                });

        System.out.println("nofQueries:"+queries.count());

        JavaPairRDD<Integer, Trie> partitionedTries = trieRDD.partitionBy(new IntegerPartitioner()).persist(StorageLevel.MEMORY_ONLY());
        JavaPairRDD<Integer, Query> partitionedQueries = queries.partitionBy(new IntegerPartitioner()).persist(StorageLevel.MEMORY_ONLY());

        JavaPairRDD<Integer, Set<Long>> resultSet = partitionedTries.join(partitionedQueries).mapValues(new Function<Tuple2<Trie, Query>, Set<Long>>() {
            @Override
            public Set<Long> call(Tuple2<Trie, Query> v1) throws Exception {
                return v1._1().queryIndex(v1._2());
            }
        }).filter(new FilterEmptyAnswers());


        List<Tuple2<Integer, Set<Long>>> lisof = resultSet.collect();
        Set<Long> uniqTrajIDs = new TreeSet<>();
        for (Tuple2<Integer, Set<Long>> t : lisof) {
            Set<Long> ff = t._2();
            for (Long l : ff) {
                uniqTrajIDs.add(l);

            }
        }

        List<Tuple2<Integer, Set<Long>>> kd = resultSet.collect();
        for (Tuple2<Integer, Set<Long>> t : kd) {
            System.out.println(t);
        }
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