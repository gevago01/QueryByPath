package partitioning.methods;

import comparators.IntegerComparator;
import comparators.LongComparator;
import map.functions.CSVRecToTrajME;
import map.functions.LineToCSVRec;
import org.apache.hadoop.yarn.util.Times;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;
import trie.Query;
import trie.Trie;
import utilities.*;

import java.util.*;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

/**
 * Created by giannis on 17/06/19.
 */
public class HybridPartitioning {
    static List<TimeSlice> allSlices=new ArrayList<>();

    public static List<Integer> determineTimeSlice(Trajectory trajectory) {
        List<Integer> timeSlices = new ArrayList<>();
        boolean foundMax = false;
        int minIndex = -1, maxIndex = -1;
        long startingTime=trajectory.getStartingTime();
        long endingTime=trajectory.getEndingTime();

        for (int i = 0; i < allSlices.size(); i++) {

            if (startingTime >= allSlices.get(i).getLowerTime()) {
                minIndex = i;
            }

            if (endingTime <= allSlices.get(i).getUpperTime() && !foundMax) {
                foundMax = true;
                maxIndex = i;
            }

        }

        //make sure you don't need equal here
        for (int i = minIndex; i <= maxIndex; i++) {
            timeSlices.add(i);
        }



        return timeSlices;

    }

    private static void initializeHybridPartitioning(JavaRDD<CSVRecord> records, int nofTimeSlices, int nofRoadSlices) {
        long minTimestamp = records.map(r -> r.getTimestamp()).min(new LongComparator());
        long maxTimestamp = records.map(r -> r.getTimestamp()).max(new LongComparator());
        long timeInterval = (maxTimestamp-minTimestamp) / nofTimeSlices;

        TimeSlice.initializeRSIntervals(records,nofRoadSlices);

        int id=0;

        for (long i = minTimestamp; i < maxTimestamp; i+=timeInterval) {
            allSlices.add(new TimeSlice(i,i+timeInterval, id++));
        }

    }

    /**
     * Returns unique slice /partition id
     * @param trajectory
     * @return
     */
    private static Integer determineSlice(final Trajectory trajectory) {
        Random random=new Random();

        List<Integer> intersectingTimeSlices = determineTimeSlice(trajectory);

        //if trajectory intersects multiple time slices return one randomly
        int timeSlice= intersectingTimeSlices.get(random.nextInt(intersectingTimeSlices.size()));

        int roadSlice =TimeSlice.determineRoadSlices(trajectory);;

        return timeSlice*roadSlice;

    }




    public static void main(String[] args) {

        int nofTimeSlices = Integer.parseInt(args[0]);
        int nofRoadSlices = Integer.parseInt(args[1]);
        int nofPartitions = nofRoadSlices*nofTimeSlices;

        String appName = HybridPartitioning.class.getSimpleName() + (nofPartitions);

//        String fileName = "hdfs:////rssd.csv";
//        String queryFile = "hdfs:////rssq.csv";
        String fileName = "hdfs:////timeSkewedDataset.csv";
        String queryFile = "hdfs:////timeSkewedQueries";
//        String fileName = "file:///mnt/hgfs/VM_SHARED/trajDatasets/onesix.csv";
//        String queryFile = "file:///mnt/hgfs/VM_SHARED/trajDatasets/onesix.csv";
//        SparkConf conf = new SparkConf().setAppName(appName).setMaster("local[*]")
//                .set("spark.executor.instances", "" + Parallelism.PARALLELISM)
//                .set("spark.driver.maxResultSize", "3G");
                SparkConf conf = new SparkConf().
                setAppName(appName);



        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<CSVRecord> records = sc.textFile(fileName).map(new LineToCSVRec());

        initializeHybridPartitioning(records, nofTimeSlices, nofRoadSlices);




        JavaPairRDD<Integer, Trajectory> trajectoryDataset = records.groupBy(csv -> csv.getTrajID()).mapValues(new CSVRecToTrajME());

//        HashMap<Integer, Integer> buckets = Intervals.hybridSlicing(trajectoryDataset, nofRoadSlices);



        trajectoryDataset = trajectoryDataset.mapToPair(trajectory -> {
            Integer bucket = HybridPartitioning.determineSlice(trajectory._2());

            trajectory._2().setPartitionID(bucket);
            return new Tuple2<>(trajectory._2().getPartitionID(), trajectory._2());
        });

        List<Tuple2<Integer, Integer>> nofTrajsList=Stats.nofTrajsOnEachNode(trajectoryDataset,sc);






        System.out.println("nofPartitions:" + nofPartitions);
        List<Integer> partitions = IntStream.range(0, nofPartitions).boxed().collect(toList());

        JavaRDD<Integer> partitionsRDD = sc.parallelize(partitions);
        JavaPairRDD<Integer, Trie> emptyTrieRDD = partitionsRDD.mapToPair(new PairFunction<Integer, Integer, Trie>() {
            @Override
            public Tuple2<Integer, Trie> call(Integer partitionID) throws Exception {
                Trie t = new Trie();
                t.setPartitionID(partitionID);
                return new Tuple2<>(partitionID, t);
            }
        });


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

        System.out.println("TrieNofPartitions:" + trieRDD.rdd().partitions().length);

        long noftries=trieRDD.count();
        System.out.println("Done. Tries build. noftries:"+noftries);

        JavaPairRDD<Integer, CSVRecord> queryRecords = sc.textFile(queryFile).map(new LineToCSVRec()).mapToPair(csvRec -> new Tuple2<>(csvRec.getTrajID(), csvRec));
        JavaPairRDD<Integer, Query> queries =
                queryRecords.groupByKey().mapValues(new CSVRecToTrajME()).map(t -> new Query(t._2())).mapToPair(q -> {

//                    q.setPartitionID(buckets.getOrDefault(q.getQueryID(),1));
                    return new Tuple2<>(q.getPartitionID(), q);
                });

        Broadcast<List<Tuple2<Integer, Query>>> partBroadQueries = sc.broadcast(queries.collect());


        JavaRDD<Set<Integer>> resultSetRDD =
                trieRDD.map(new Function<Tuple2<Integer, Trie>, Set<Integer>>() {
                    @Override
                    public Set<Integer> call(Tuple2<Integer, Trie> v1) throws Exception {
                        Set<Integer> answer = new TreeSet<>();

                        List<Tuple2<Integer, Query>> localQueries = partBroadQueries.value();

                        for (Tuple2<Integer, Query> queryEntry : localQueries) {

                            long t1 = System.nanoTime();
                            Set<Integer> ans = v1._2().queryIndex(queryEntry._2());
                            long t2 = System.nanoTime();
                            if (!ans.isEmpty()){
//                                    System.out.println("iid:"+v1._1()+","+ans.size());
                            }
                            long diff = t2 - t1;
                            answer.addAll(ans);
                        }
                        return answer;
                    }
                }).filter(set -> set.isEmpty() ? false : true);
        List<Integer> collectedResult = resultSetRDD.collect().stream().flatMap(set -> set.stream()).distinct().collect(toList());

        for (Integer t : collectedResult) {
            System.out.println(t);
        }

        System.out.println("resultSetSize:"+collectedResult.size());


    }




}
