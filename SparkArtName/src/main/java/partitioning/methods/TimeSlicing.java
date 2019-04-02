package partitioning.methods;

import comparators.IntegerComparator;
import comparators.LongComparator;
import key.selectors.CSVTrajIDSelector;
import map.functions.CSVRecToTrajME;
import map.functions.LineToCSVRec;
import map.functions.TrajToTSTrajs;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.LongAccumulator;
import projections.ProjectTimestamps;
import scala.Tuple2;
import trie.Query;
import trie.Trie;
import utilities.*;

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
        int numberOfSlices = Integer.parseInt(args[0]);
        ++numberOfSlices;

        String appName = TimeSlicing.class.getSimpleName() + numberOfSlices;


//        String fileName = "file:///mnt/hgfs/VM_SHARED/samplePort.csv";
//        String queryFile = "file:///mnt/hgfs/VM_SHARED/samplePort.csv";
//        String fileName= "file:///mnt/hgfs/VM_SHARED/trajDatasets/octant.csv";
//        String fileName= "file:///mnt/hgfs/VM_SHARED/trajDatasets/half.csv";
//        String fileName= "file:///mnt/hgfs/VM_SHARED/trajDatasets/85TD.csv";

//        String fileName = "file:///mnt/hgfs/VM_SHARED/trajDatasets/concatTrajectoryDataset.csv";
//        String fileName = "file:////data/half.csv";
//        String fileName= "hdfs:////half.csv";
//        String fileName= "hdfs:////onesix.csv";
//        String fileName= "hdfs:////octant.csv";
//        String fileName = "file:///mnt/hgfs/VM_SHARED/trajDatasets/onesix.csv";
//        String queryFile = "file:///mnt/hgfs/VM_SHARED/trajDatasets/onesix.csv";
//        String queryFile = "file:///mnt/hgfs/VM_SHARED/trajDatasets/onesix.csv";
//        String queryFile = "file:///mnt/hgfs/VM_SHARED/trajDatasets/queryRecords.csv";
//        String fileName= "hdfs:////synth8GBDataset.csv";
//        String fileName= "hdfs:////synth5GBDataset.csv";
//        String queryFile = "hdfs:////queryRecords.csv";

        String fileName = "hdfs:////concatTrajectoryDataset.csv";
        String queryFile = "hdfs:////200KqueryRecords.csv";
//        String queryFile = "hdfs:////queryRecordsOnesix.csv";


//        String fileName= "file:////home/giannis/octant.csv";

//        SparkConf conf = new SparkConf().setAppName(appName).setMaster("local[*]")
//                .set("spark.executor.instances", "" + Parallelism.PARALLELISM)
//                .set("spark.driver.maxResultSize", "3G");
        SparkConf conf = new SparkConf().setAppName(appName);

        JavaSparkContext sc = new JavaSparkContext(conf);
//        sc.setCheckpointDir("/home/giannis/IdeaProjects/SparkTrajectories/SparkArtName/target/cpdir");
        LongAccumulator nofQueries = sc.sc().longAccumulator("NofQueries");
        DoubleAccumulator joinTimeAcc = sc.sc().doubleAccumulator("joinTimeAcc");


//        CustomListener mylistener=new CustomListener();
        JavaRDD<CSVRecord> records = sc.textFile(fileName).map(new LineToCSVRec());

        JavaRDD<Long> timestamps = records.map(new ProjectTimestamps());
        JavaRDD<Integer> trajIDs = records.map(r -> r.getTrajID());

        timestamps.count();
        long minTimestamp = timestamps.min(new LongComparator());
        long maxTimestamp = timestamps.max(new LongComparator());
        //timestamps RDD not needed any more
        timestamps.unpersist(true);

        List<Long> timePeriods = Intervals.getIntervals(numberOfSlices, minTimestamp, maxTimestamp);


        JavaPairRDD<Integer, Trajectory> trajectoryDataset = records.groupBy(new CSVTrajIDSelector()).mapValues(new CSVRecToTrajME()).flatMapValues(new TrajToTSTrajs(timePeriods)).mapToPair(pair -> new Tuple2<>(pair._2().getTimeSlice(), pair._2()));//filter(new ReduceNofTrajectories());

        trajectoryDataset.count();

        //records RDD not needed any more
        records.unpersist(true);
//        GroupSizes.mesaureGroupSize(trajectoryDataset, TimeSlicing.class.getName());


        assert (trajectoryDataset.keys().distinct().count() == numberOfSlices);

        List<Integer> partitions = IntStream.range(0, numberOfSlices).boxed().collect(toList());
        JavaRDD<Integer> partitionsRDD = sc.parallelize(partitions);
        JavaPairRDD<Integer, Trie> emptyTrieRDD = partitionsRDD.mapToPair(new PairFunction<Integer, Integer, Trie>() {
            @Override
            public Tuple2<Integer, Trie> call(Integer timeSlice) throws Exception {
                Trie t = new Trie();
                t.setTimeSlice(timeSlice);
                return new Tuple2<>(timeSlice, t);
            }
        });

        assert (partitions.size() == numberOfSlices && partitionsRDD.count() == numberOfSlices);


        JavaRDD<Integer> setDifference = emptyTrieRDD.keys().subtract(trajectoryDataset.keys().distinct());

        int maxTimeSlice = trajectoryDataset.keys().max(new IntegerComparator());

        System.out.println("maxTimeSlice:" + maxTimeSlice);


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

        Stats.nofTrajsOnEachNode(trajectoryDataset, PartitioningMethods.TIME_SLICING);
//        Stats.nofTrajsInEachTS(trajectoryDataset);
        //trajectoryDataset RDD not needed any more
        trajectoryDataset.unpersist(true);

        trieRDD.count();
        System.out.println("Done. Tries build");
//        Query q = new Query(trajectory.getStartingTime(), trajectory.getEndingTime(), trajectory.roadSegments);
//        List<Integer> times_slices = q.determineTimeSlice(timePeriods);
//        q.timeSlice = times_slices.get(new Random().nextInt(times_slices.size()));


        JavaPairRDD<Integer, CSVRecord> queryRecords = sc.textFile(queryFile).map(new LineToCSVRec()).mapToPair(csvRec -> new Tuple2<>(csvRec.getTrajID(), csvRec));
        JavaPairRDD<Integer, Query> queries =
                queryRecords.groupByKey().mapValues(new CSVRecToTrajME()).map(t -> new Query(t._2(), timePeriods)).mapToPair(q -> new Tuple2<>(q.getTimeSlice(), q));
//        Broadcast<Map<Integer, Query>> broadcastedQueries = sc.broadcast(queries.collectAsMap());
        System.out.println("nofQueries:" + queries.count());
        System.out.println("appName:" + appName);

        System.out.println("TrieNofPartitions:" + trieRDD.rdd().partitions().length);
        Stats.nofQueriesOnEachNode(queries, PartitioningMethods.TIME_SLICING);
//        Stats.nofQueriesInEachTS(queries);
//        Stats.nofTriesInPartitions(trieRDD);
        Broadcast<List<Tuple2<Integer, Query>>> partBroadQueries = sc.broadcast(queries.collect());
        JavaRDD<Set<Integer>> resultSetRDD =
                trieRDD.map(new Function<Tuple2<Integer, Trie>, Set<Integer>>() {
                    @Override
                    public Set<Integer> call(Tuple2<Integer, Trie> v1) throws Exception {
                        Set<Integer> answer = new TreeSet<>();

                        List<Tuple2<Integer, Query>> localQueries = partBroadQueries.value();


                        for (Tuple2<Integer, Query> queryEntry : localQueries) {
                            if (v1._2().getTimeSlice() == queryEntry._2().getTimeSlice()) {

                                long t1 = System.nanoTime();
                                Set<Integer> ans = v1._2().queryIndex(queryEntry._2());
                                long t2 = System.nanoTime();
                                long diff = t2 - t1;
                                joinTimeAcc.add(diff * Math.pow(10.0, -9.0));
                                answer.addAll(ans);
                            }
                        }
                        return answer;
                    }
                }).filter(set -> set == null ? false : true);


        List<Set<Integer>> resultSet = resultSetRDD.collect();
        int i = 0;
        for (Set<Integer> trajID : resultSet) {
            System.out.println(trajID);
            i += trajID.size();
        }
        System.out.println("Size of resultSet:" + i);

//        -- using a broadcast variable
//
//        partitionedTries.groupWith(partitionedQueries).foreach(new VoidFunction<Tuple2<Integer, Tuple2<Iterable<Trie>, Iterable<Query>>>>() {
//            @Override
//            public void call(Tuple2<Integer, Tuple2<Iterable<Trie>, Iterable<Query>>> trieQueryTuple) throws Exception {
//                List<Query> queryList=Lists.newArrayList(trieQueryTuple._2()._2());
//
//                Set<Long> answer = new TreeSet<>();
//
//
//                for(Trie trie:trieQueryTuple._2()._1()) {
//                    for (int i = 0; i < queryList.size(); i++) {
//                        answer.addAll(trie.queryIndex(queryList.get(i)));
//                    }
//
//
//                }
//
//                System.out.println("answer.size():"+answer.size());
//
//            }
//        });

//        List<Set<Long>> resultSet=trieDataset.mapValues(new Function<Tuple2<Integer, Trie>, Set<Long>>() {
//            @Override
//            public Set<Long> call(Tuple2<Integer, Trie> v1) throws Exception {
//                Map<Integer, Query> localQueries = broadcastedQueries.value();
//
//                Set<Long> answer=new TreeSet<>();
//                for (Map.Entry<Integer, Query> queryEntry : localQueries.entrySet()) {
//
//                    if (queryEntry.getValue().getTimeSlice()==v1._2().getTimeSlice()){
//                        answer.addAll(v1._2().queryIndex(queryEntry.getValue()));
//                    }
//                    else{
//                        System.out.println("NOTFOUND");
//                    }
//                }
//                return answer;
//            }
//        }).filter( set -> set.isEmpty()?false:true).collect();
//        for (Set<Long> set:resultSet) {
//            System.out.println(set);
//        }
//        System.out.println("Size of resultSet:"+resultSet.size());


    }
}
