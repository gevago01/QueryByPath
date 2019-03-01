package partitioning.methods;

import comparators.LongComparator;
import filtering.FilterEmptyAnswers;
import filtering.FilterNullQueries;
import filtering.ReduceNofTrajectories;
import key.selectors.CSVTrajIDSelector;
import key.selectors.TrajectoryTSSelector;
import map.functions.CSVRecToTrajME;
import map.functions.LineToCSVRec;
import map.functions.TrajToTSTrajs;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.LongAccumulator;
import org.spark_project.guava.collect.Lists;
import partitioners.IntegerPartitioner;
import projections.ProjectTimestamps;
import scala.Tuple2;
import trie.Query;
import trie.Trie;
import utilities.CSVRecord;
import utilities.CustomListener;
import utilities.Parallelism;
import utilities.Trajectory;

import java.util.*;

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
        String fileName= "file:///mnt/hgfs/VM_SHARED/trajDatasets/half.csv";
//        String fileName= "file:///mnt/hgfs/VM_SHARED/trajDatasets/85TD.csv";
//        String fileName = "file:///mnt/hgfs/VM_SHARED/trajDatasets/onesix.csv";
//          String fileName= "file:////home/giannis/concatTrajectoryDataset.csv";
//        String fileName = "file:////data/half.csv";
//        String fileName= "hdfs:////half.csv";
//        String fileName = "hdfs:////concatTrajectoryDataset.csv";
//        String fileName= "hdfs:////onesix.csv";
//        String fileName= "hdfs:////octant.csv";


//        String fileName= "file:////home/giannis/octant.csv";

        SparkConf conf = new SparkConf().setAppName(appName).setMaster("local[*]").
                set("spark.executor.instances", "" + Parallelism.PARALLELISM);
//        SparkConf conf = new SparkConf().setAppName(appName);


        JavaSparkContext sc = new JavaSparkContext(conf);
        LongAccumulator nofQueries = sc.sc().longAccumulator("NofQueries");
        DoubleAccumulator joinTimeAcc = sc.sc().doubleAccumulator("joinTimeAcc");

//        sc.sc().addSparkListener(new CustomListener());
        JavaRDD<CSVRecord> records = sc.textFile(fileName).map(new LineToCSVRec());

        JavaRDD<Long> timestamps = records.map(new ProjectTimestamps());

        timestamps.count();
        long min = timestamps.min(new LongComparator());
        long max = timestamps.max(new LongComparator());

        List<Long> timePeriods = getTimeIntervals(numberOfSlices, min, max);


        JavaPairRDD<Long, Trajectory> trajectoryDataset = records.groupBy(new CSVTrajIDSelector()).mapValues(new CSVRecToTrajME()).filter(new ReduceNofTrajectories()).flatMapValues(new TrajToTSTrajs(timePeriods));

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
        JavaPairRDD<Integer, Query> queries =
                trajectoryDataset.values().map(new Function<Trajectory, Query>() {

                    private Random random = new Random();

                    private final static float PROBABILITY = 0.00065f;
//                    private final static float PROBABILITY = 1f;

                    private boolean getRandomBoolean(float p) {

                        return random.nextFloat() < p;
                    }

                    @Override
                    public Query call(Trajectory trajectory) throws Exception {
                        if (getRandomBoolean(PROBABILITY)) {
                            Query q = new Query(trajectory.getStartingTime(), trajectory.getEndingTime(), trajectory.roadSegments);
                            List<Integer> times_slices = q.determineTimeSlice(timePeriods);
                            //if query considers a timespan that belongs to two tries
                            //choose a random trie to query
                            q.timeSlice = times_slices.get(new Random().nextInt(times_slices.size()));
                            nofQueries.add(1l);

                            return q;
                        }
                        return null;
                    }
                }).filter(new FilterNullQueries()).mapToPair(new PairFunction<Query, Integer, Query>() {
                    @Override
                    public Tuple2<Integer, Query> call(Query query) throws Exception {
                        return new Tuple2<>(query.getTimeSlice(), query);
                    }
                });

        queries.saveAsObjectFile("queries.o");
        System.out.println("nofQueries:" + queries.count());
        System.exit(1);

        Broadcast<Map<Integer, Query>> broadcastedQueries = sc.broadcast(queries.collectAsMap());
        System.out.println("nofQueries:" + queries.count());
        System.out.println("appName:" + appName);
        JavaPairRDD<Integer, Trie> partitionedTries = trieDataset.partitionBy(new IntegerPartitioner(numberOfSlices)).persist(StorageLevel.MEMORY_ONLY());
        JavaPairRDD<Integer, Query> partitionedQueries = queries.partitionBy(new IntegerPartitioner(numberOfSlices)).persist(StorageLevel.MEMORY_ONLY());

//        Stats.nofQueriesInEachTimeSlice(partitionedQueries);
//        Stats.nofTriesInPartitions(trieDataset);

        JavaPairRDD<Integer, Set<Long>> resultSetRDD = partitionedTries.join(partitionedQueries).mapValues(new Function<Tuple2<Trie, Query>, Set<Long>>() {
            @Override
            public Set<Long> call(Tuple2<Trie, Query> trieQueryTuple) throws Exception {

                long t1=System.nanoTime();
                Set<Long> result=trieQueryTuple._1().queryIndex(trieQueryTuple._2());
                long t2=System.nanoTime();
                long diff=t2-t1;
                joinTimeAcc.add(diff*Math.pow(10.0,-9.0));;
                return result;
            }
        }).filter(new FilterEmptyAnswers());



        List<Tuple2<Integer, Set<Long>>> resultSet=resultSetRDD.collect();
        int sum=0;
        for (Tuple2<Integer, Set<Long>> ttuple:resultSet) {

            System.out.println(ttuple._2());
            sum+=ttuple._2().size();
        }
        System.out.println("Size of resultSet:"+sum);

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
