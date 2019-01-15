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
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.LongAccumulator;
import partitioners.IntegerPartitioner;
import projections.ProjectTimestamps;
import scala.Tuple2;
import trie.Query;
import trie.Trie;
import utilities.CSVRecord;
import utilities.Parallelism;
import utilities.Stats;
import utilities.Trajectory;

import java.util.*;

/**
 * Created by giannis on 10/12/18.
 */
public class TimeSlicing {


    //for full dataset
//    private static final Long MIN_TIME = 1372636853000l;
//    private static final Long MAX_TIME = 1404172759000l;
    //for half dataset
//    private static final Long MIN_TIME = 1372636853000l;
//    private static final Long MAX_TIME = 1404172751000l;
    //for octant - 1/4
//    private static final Long MIN_TIME = 1372636951000l;
//    private static final Long MAX_TIME = 1404172591000l;
    //for onesix
    private static final Long MIN_TIME = 1372636951000l;
    private static final Long MAX_TIME = 1404172591000l;
    //for sample
//    private static final Long MIN_TIME = 1376904623000l;
//    private static final Long MAX_TIME = 1376916881000l;

    //    private static final Long MIN_TIME = 1372636951000l;
//    private static final Long MAX_TIME = 1404172591000l;

    public static List<Long> getTimeIntervals(long NUMBER_OF_SLICES, long MIN_TIME,long MAX_TIME) {
        final Long timeInt = (MAX_TIME - MIN_TIME) / NUMBER_OF_SLICES;

        List<Long> timePeriods = new ArrayList<>();
        for (long i = MIN_TIME; i < MAX_TIME; i += timeInt) {
            timePeriods.add(i);
        }
        timePeriods.add(MAX_TIME);

        System.out.println("Using:::"+MIN_TIME+","+MAX_TIME);
        return timePeriods;
    }

    public static void main(String[] args) {
        int numberOfSlices = Integer.parseInt(args[0]);
        String appName=HorizontalPartitioning.class.getSimpleName()+numberOfSlices;
//        String fileName = "file:///mnt/hgfs/VM_SHARED/samplePort.csv";
//        String fileName= "file:///mnt/hgfs/VM_SHARED/trajDatasets/octant.csv";
//        String fileName= "file:///mnt/hgfs/VM_SHARED/trajDatasets/half.csv";
//        String fileName= "file:///mnt/hgfs/VM_SHARED/trajDatasets/85TD.csv";
        String fileName= "file:///mnt/hgfs/VM_SHARED/trajDatasets/onesix.csv";
//          String fileName= "file:////home/giannis/concatTrajectoryDataset.csv";
//        String fileName = "file:////data/half.csv";
//        String fileName= "hdfs:////half.csv";
//        String fileName = "hdfs:////concatTrajectoryDataset.csv";
//        String fileName= "hdfs:////onesix.csv";
//        String fileName= "hdfs:////octant.csv";

//        String fileName= "file:////home/giannis/octant.csv";

        SparkConf conf = new SparkConf().setAppName(appName).setMaster("local[*]").
                set("spark.executor.instances", "" + Parallelism.PARALLELISM);
//        SparkConf conf = new SparkConf().setAppName(appName)
//                .set("spark.executor.instances", "" + Parallelism.PARALLELISM)
//                .set("spark.executor.cores", "" + Parallelism.EXECUTOR_CORES);//.set("spark.executor.heartbeatInterval","15s");

        JavaSparkContext sc = new JavaSparkContext(conf);
        LongAccumulator nofQueries=sc.sc().longAccumulator("NofQueries");

        JavaRDD<CSVRecord> records = sc.textFile(fileName).map(new LineToCSVRec());

        JavaRDD<Long> timestamps = records.map(new ProjectTimestamps());

        timestamps.count();
        long min=timestamps.min(new LongComparator());
        long max=timestamps.max(new LongComparator());

//        System.out.println("min:"+min);
//        System.out.println("max:"+max);
        List<Long> timePeriods = getTimeIntervals(numberOfSlices,min,max);

        JavaPairRDD<Long, Trajectory> trajectoryDataset = records.groupBy(new CSVTrajIDSelector()).mapValues(new CSVRecToTrajME()).filter(new ReduceNofTrajectories()).flatMapValues(new TrajToTSTrajs(timePeriods));
        JavaPairRDD<Integer, Trie> trieDataset = trajectoryDataset. values().groupBy(new TrajectoryTSSelector()).flatMapValues(new Function<Iterable<Trajectory>, Iterable<Trie>>() {
            @Override
            public Iterable<Trie> call(Iterable<Trajectory> trajectories) throws Exception {
                List<Trie> trieList = new ArrayList<>();
                Trie trie = new Trie();
                for (Trajectory t : trajectories) {

                    trie.insertTrajectory2(t.roadSegments, t.trajectoryID, t.getStartingTime(), t.getEndingTime());

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

//                    private final static float PROBABILITY = 0.0002f;
                    private final static float PROBABILITY = 1f;

                    private boolean getRandomBoolean(float p) {

                        return random.nextFloat() < p;
                    }

                    @Override
                    public Query call(Trajectory trajectory) throws Exception {
                        if (getRandomBoolean(PROBABILITY)) {
                            Query q = new Query(trajectory.getStartingTime(),trajectory.getEndingTime(),trajectory.roadSegments);
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


        System.out.println("nofQueries:"+queries.count());
        System.out.println("appName:"+appName);
        JavaPairRDD<Integer, Trie> partitionedTries = trieDataset.partitionBy(new IntegerPartitioner()).persist(StorageLevel.MEMORY_ONLY());
        JavaPairRDD<Integer, Query> partitionedQueries = queries.partitionBy(new IntegerPartitioner()).persist(StorageLevel.MEMORY_ONLY());

        Stats.nofTriesInPartitions(partitionedTries);

        JavaPairRDD<Integer, Set<Long>> resultSet = partitionedTries.join(partitionedQueries).mapValues(new Function<Tuple2<Trie, Query>, Set<Long>>() {
            @Override
            public Set<Long> call(Tuple2<Trie, Query> trieQueryTuple) throws Exception {
                return trieQueryTuple._1().queryIndex(trieQueryTuple._2());
            }
        }).filter(new FilterEmptyAnswers());

        List<Tuple2<Integer, Set<Long>>> lisof = resultSet.collect();
        for (Tuple2<Integer, Set<Long>> t : lisof) {
            System.out.println(t);

        }

        System.out.println("resultSetSize:"+lisof.size());

    }
}
