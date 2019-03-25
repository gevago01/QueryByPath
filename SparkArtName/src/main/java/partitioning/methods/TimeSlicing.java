package partitioning.methods;

import comparators.IntegerComparator;
import comparators.LongComparator;
import filtering.FilterEmptyAnswers;
import filtering.ReduceNofTrajectories;
import key.selectors.CSVTrajIDSelector;
import key.selectors.TrajectoryTSSelector;
import map.functions.CSVRecToTrajME;
import map.functions.CSVRecordToTrajectory;
import map.functions.LineToCSVRec;
import map.functions.TrajToTSTrajs;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.LongAccumulator;
import partitioners.IntegerPartitioner;
import projections.ProjectTimestamps;
import scala.Tuple2;
import trie.Query;
import trie.Trie;
import utilities.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Created by giannis on 10/12/18.
 */
public class TimeSlicing {


    public static List<Long> getTimeIntervals(long numberOfSlices, long minTime, long maxTime) {
        final Long timeInt = (maxTime - minTime) / numberOfSlices;

        List<Long> timePeriods = new ArrayList<>();
        for (long i = minTime; i < maxTime; i += timeInt) {
            timePeriods.add(i);
        }
        timePeriods.add(maxTime);


        return timePeriods;
    }

    public static void main(String[] args) {
        int numberOfSlices = Integer.parseInt(args[0]);


        String appName = TimeSlicing.class.getSimpleName() + numberOfSlices;


//        String fileName = "file:///mnt/hgfs/VM_SHARED/samplePort.csv";
//        String fileName= "file:///mnt/hgfs/VM_SHARED/trajDatasets/octant.csv";
//        String fileName= "file:///mnt/hgfs/VM_SHARED/trajDatasets/half.csv";
//        String fileName= "file:///mnt/hgfs/VM_SHARED/trajDatasets/85TD.csv";

//        String fileName = "file:///mnt/hgfs/VM_SHARED/trajDatasets/concatTrajectoryDataset.csv";
//        String fileName = "file:////data/half.csv";
//        String fileName= "hdfs:////half.csv";
//        String fileName = "hdfs:////concatTrajectoryDataset.csv";
//        String fileName= "hdfs:////onesix.csv";
//        String fileName= "hdfs:////octant.csv";
//        String fileName = "file:///mnt/hgfs/VM_SHARED/trajDatasets/onesix.csv";
//        String fileName = "file:///mnt/hgfs/VM_SHARED/trajDatasets/onesix.csv";
//        String queryFile = "file:///mnt/hgfs/VM_SHARED/trajDatasets/onesix.csv";
//        String queryFile = "file:///mnt/hgfs/VM_SHARED/trajDatasets/queryRecords.csv";
//        String fileName= "hdfs:////synth8GBDataset.csv";
        String fileName= "hdfs:////synth5GBDataset.csv";
        String queryFile = "hdfs:////queryRecords.csv";

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

        long maxTrajID = trajIDs.max(new IntegerComparator());
        timestamps.count();
        long minTimestamp = timestamps.min(new LongComparator());
        long maxTimestamp = timestamps.max(new LongComparator());


        List<Long> timePeriods = getTimeIntervals(numberOfSlices, minTimestamp, maxTimestamp);


        JavaPairRDD<Integer, Trajectory> trajectoryDataset = records.groupBy(new CSVTrajIDSelector()).mapValues(new CSVRecToTrajME()).flatMapValues(new TrajToTSTrajs(timePeriods)).filter(new ReduceNofTrajectories());
//        JavaPairRDD<Long, Trajectory> trajectoryDataset = records.groupBy(new CSVTrajIDSelector()).mapValues(new CSVRecordToTrajectory());
//        try {
//            TrajectorySynthesizer.synthesize(trajectoryDataset.values().collect(), minTimestamp, maxTimestamp, maxTrajID);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        System.exit(1);
        JavaPairRDD<Integer, Trie> trieDataset = trajectoryDataset.values().groupBy(new TrajectoryTSSelector()).flatMapValues(new Function<Iterable<Trajectory>, Iterable<Trie>>() {
            @Override
            public Iterable<Trie> call(Iterable<Trajectory> trajectories) throws Exception {
                List<Trie> trieList = new ArrayList<>();
                Trie trie = new Trie();
                for (Trajectory t : trajectories) {

                    trie.insertTrajectory2(t);

                    trie.timeSlice = t.timeSlice;
                }

                trieList.add(trie);
                return trieList;
            }
        });

        System.out.println("Done. Tries build");
        //done with records - unpersist
//        Query q = new Query(trajectory.getStartingTime(), trajectory.getEndingTime(), trajectory.roadSegments);
//        List<Integer> times_slices = q.determineTimeSlice(timePeriods);
//        q.timeSlice = times_slices.get(new Random().nextInt(times_slices.size()));


//        trieDataset.cache();
//        trieDataset.checkpoint();
        JavaPairRDD<Integer, CSVRecord> queryRecords = sc.textFile(queryFile).map(new LineToCSVRec()).mapToPair(csvRec -> new Tuple2<>(csvRec.getTrajID(), csvRec));

        JavaPairRDD<Integer, Query> queries =
                queryRecords.groupByKey().mapValues(new CSVRecToTrajME()).map(t -> new Query(t._2(), timePeriods)).mapToPair(q -> new Tuple2<>(q.getTimeSlice(), q));



//        Broadcast<Map<Integer, Query>> broadcastedQueries = sc.broadcast(queries.collectAsMap());
        System.out.println("nofQueries:" + queries.count());
        System.out.println("appName:" + appName);
        JavaPairRDD<Integer, Trie> partitionedTries = trieDataset.partitionBy(new IntegerPartitioner()).persist(StorageLevel.MEMORY_ONLY());
        System.out.println("nofTries:" + partitionedTries.count());

        System.out.println("TrieNofPartitions:" + partitionedTries.rdd().partitions().length);
        JavaPairRDD<Integer, Query> partitionedQueries = queries.partitionBy(new IntegerPartitioner()).persist(StorageLevel.MEMORY_ONLY());
        partitionedQueries.count();



//        Stats.nofQueriesInEachTimeSlice(partitionedQueries);
//        Stats.nofTriesInPartitions(partitionedTries);
        Broadcast<List<Tuple2<Integer, Query>>> partBroadQueries=sc.broadcast(queries.collect());
        JavaRDD<Set<Integer>> resultSetRDD =
                partitionedTries.map(new Function<Tuple2<Integer,Trie>, Set<Integer>>() {
                    @Override
                    public Set<Integer> call(Tuple2<Integer, Trie> v1) throws Exception {
                        List<Tuple2<Integer, Query>> localQueries=partBroadQueries.value();
                        Set<Integer> answer=new TreeSet<>();

                        for (Tuple2<Integer, Query> queryEntry:localQueries) {
                            if (v1._2().getTimeSlice()==queryEntry._2().getTimeSlice()) {
                                Set<Integer> ans=v1._2().queryIndex(queryEntry._2());
                                answer.addAll(ans);
                            }
                        }
                        return answer;
                    }
                }).filter(set -> set.isEmpty()? false: true);


        List<Set<Integer>> resultSet = resultSetRDD.collect();
        int i=0;
        for (Set<Integer> trajID : resultSet) {
            System.out.println(trajID);
            i+=trajID.size();
        }
        System.out.println("Size of resultSet:" + i);

//        -- using a broadcast variable

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
