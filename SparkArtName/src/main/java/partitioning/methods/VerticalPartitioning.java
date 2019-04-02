package partitioning.methods;

import comparators.IntegerComparator;
import key.selectors.CSVTrajIDSelector;
import map.functions.CSVRecToTrajME;
import map.functions.LineToCSVRec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.LongAccumulator;
import partitioners.StartingRSPartitioner;
import projections.ProjectRoadSegments;
import scala.Tuple2;
import trie.Query;
import trie.Trie;
import utilities.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;


/**
 * Created by giannis on 10/12/18.
 */
public class VerticalPartitioning {


    private static List<Integer> sliceNofSegments2(JavaRDD<CSVRecord> records, int nofVerticalSlices) {
        JavaRDD<Integer> roadSegments = records.map(new ProjectRoadSegments()).distinct().sortBy(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                return v1;
            }
        }, true, 1);

        int min = roadSegments.min(new IntegerComparator());
        int max = roadSegments.max(new IntegerComparator());

        long timePeriod = max - min;

        long interval = timePeriod / nofVerticalSlices;

        List<Integer> roadIntervals = new ArrayList<>();
        for (int i = min; i < max; i += interval) {
            roadIntervals.add(i);
        }
        roadIntervals.remove(roadIntervals.size() - 1);
        roadIntervals.add(max);


        return roadIntervals;
    }


//    private static List<Long> sliceNofSegments(JavaRDD<CSVRecord> records, int nofVerticalSlices) {
//        JavaRDD<Integer> roadSegments = records.map(new ProjectRoadSegments()).distinct().sortBy(new Function<Integer, Integer>() {
//            @Override
//            public Long call(Integer v1) throws Exception {
//                return v1;
//            }
//        }, true, 1);
//        long nofSegments = roadSegments.count();
//        JavaPairRDD<Integer, Integer> roadSegmentswIndex =
//                roadSegments.zipWithIndex().mapToPair(new PairFunction<Tuple2<Integer, Integer>, Integer, Integer>() {
//                    @Override
//                    public Tuple2<Integer, Integer> call(Tuple2<Long, Long> t) throws Exception {
//                        return new Tuple2<>(t._2(), t._1());
//                    }
//                });
//
//        long bucketCapacity = nofSegments / nofVerticalSlices;
//
//        List<Long> indices = new ArrayList<>();
//        for (long i = 0; i < nofSegments - 1; i += bucketCapacity) {
//            indices.add(i);
//        }
//        indices.add(nofSegments - 1);
//        List<Long> roadIntervals = new ArrayList<>();
//        for (Long l : indices) {
//            roadIntervals.add(roadSegmentswIndex.lookup(l).get(0));
//        }
//
//        return roadIntervals;
//    }


    public static void main(String[] args) {


        int nofVerticalSlices = Integer.parseInt(args[0]);
//        String fileName = "file:///mnt/hgfs/VM_SHARED/samplePort.csv";
//        String fileName = "file:////mnt/hgfs/VM_SHARED/trajDatasets/concatTrajectoryDataset.csv";
//          String fileName= "file:////mnt/hgfs/VM_SHARED/trajDatasets/half.csv";
//        String fileName= "hdfs:////half.csv";
//        String fileName= "hdfs:////85TD.csv";


//        String fileName= "file:////home/giannis/octant.csv";
//        String queryFile = "file:////mnt/hgfs/VM_SHARED/trajDatasets/onesix.csv";
//        String fileName = "file:////mnt/hgfs/VM_SHARED/trajDatasets/onesix.csv";
//        String fileName = "file:///mnt/hgfs/VM_SHARED/samplePort.csv";
//        String queryFile = "file:///mnt/hgfs/VM_SHARED/samplePort.csv";;
//        String queryFile = "file:///mnt/hgfs/VM_SHARED/samplePort.csv";
//        String queryFile = "file:///mnt/hgfs/VM_SHARED/trajDatasets/queryRecords.csv";
//        String queryFile = "file:////data/queryRecords.csv";
//        String fileName= "hdfs:////synth5GBDataset.csv";
//        String queryFile = "hdfs:////queryRecords.csv";
//        String queryFile = "hdfs:////200KqueryRecords.csv";
//        String queryFile = "hdfs:////queryRecordsOnesix.csv";
        String fileName = "hdfs:////concatTrajectoryDataset.csv";
        String queryFile = "hdfs:////200KqueryRecords.csv";
//        String fileName= "hdfs:////onesix.csv";
//        String queryFile = "hdfs:////onesix.csv";
//        SparkConf conf = new SparkConf().setAppName(VerticalPartitioning.class.getSimpleName() + nofVerticalSlices)
//                .setMaster("local[*]")
//                .set("spark.executor.instances", "" + Parallelism.PARALLELISM);
        SparkConf conf = new SparkConf().setAppName(VerticalPartitioning.class.getSimpleName() + nofVerticalSlices);

        JavaSparkContext sc = new JavaSparkContext(conf);
        LongAccumulator queryLengthSum = sc.sc().longAccumulator("queryLengthSum");
        JavaRDD<CSVRecord> records = sc.textFile(fileName).map(new LineToCSVRec());
        DoubleAccumulator joinTimeAcc = sc.sc().doubleAccumulator("joinTimeAcc");

//        JavaRDD<Long> roadSegments = records.map(new ProjectRoadSegments()).sortBy(null,true,1);


        List<Integer> roadIntervals = sliceNofSegments2(records, nofVerticalSlices);

        JavaPairRDD<Integer, CSVRecord> queryRecords = sc.textFile(queryFile, Parallelism.PARALLELISM).map(new LineToCSVRec()).mapToPair(csvRec -> new Tuple2<>(csvRec.getTrajID(), csvRec));



        JavaPairRDD<Integer, Query> queries =
                queryRecords.groupByKey().mapValues(new CSVRecToTrajME()).map(t -> new Query(t._2(), roadIntervals, PartitioningMethods.VERTICAL)).mapToPair(q -> new Tuple2<>(q.getVerticalID(), q));

        System.out.println("nofQueries:"+queries.count());


        JavaPairRDD<Integer, Trajectory> trajectoryDataset = records.groupBy(new CSVTrajIDSelector()).mapValues(new CSVRecToTrajME()).mapToPair(new PairFunction<Tuple2<Integer, Trajectory>, Integer, Trajectory>() {
            @Override
            public Tuple2<Integer, Trajectory> call(Tuple2<Integer, Trajectory> trajectoryTuple2) throws Exception {
                int x = new StartingRSPartitioner(roadIntervals, nofVerticalSlices).getPartition2(trajectoryTuple2._2().getStartingRS());

                trajectoryTuple2._2().setVerticalID(x);
                return new Tuple2<Integer, Trajectory>(x, trajectoryTuple2._2());
//                return new Tuple2<Integer, Trajectory>(trajectoryTuple2._2().getStartingRS(), trajectoryTuple2._2());
            }
        });//.filter(new ReduceNofTrajectories());

        trajectoryDataset.count();
        System.out.println("Done. Trajectories build");


        List<Integer> partitions = IntStream.range(0, nofVerticalSlices).boxed().collect(toList());
        JavaRDD<Integer> partitionsRDD = sc.parallelize(partitions);
        JavaPairRDD<Integer, Trie> emptyTrieRDD = partitionsRDD.mapToPair(new PairFunction<Integer, Integer, Trie>() {
            @Override
            public Tuple2<Integer, Trie> call(Integer verticalID) throws Exception {
                Trie t = new Trie();
                t.setVerticalID(verticalID);
                return new Tuple2<>(verticalID, t);
            }
        });

        Stats.nofQueriesOnEachNode(queries, PartitioningMethods.VERTICAL);
        Stats.nofTrajsOnEachNode(trajectoryDataset,PartitioningMethods.VERTICAL);

        JavaPairRDD<Integer, Trie> trieRDD=
                emptyTrieRDD.groupWith(trajectoryDataset).mapValues(new Function<Tuple2<Iterable<Trie>, Iterable<Trajectory>>, Trie>() {
                    @Override
                    public Trie call(Tuple2<Iterable<Trie>, Iterable<Trajectory>> v1) throws Exception {
                        Trie trie=v1._1().iterator().next();
                        for (Trajectory trajectory: v1._2()){
                            trie.insertTrajectory2(trajectory);
                        }
                        return trie;
                    }
                });


        System.out.println("nofTries:"+trieRDD.count());






        System.out.println("TrieNofPartitions:" + trieRDD.rdd().partitions().length);

//
//        List<Set<Integer>> collectedResult = resultSet.values().collect();
//        collectedResult.stream().forEach(System.out::println);
//        System.out.println("resultSetSize:" + collectedResult.size());



        Broadcast<List<Tuple2<Integer, Query>>> partBroadQueries = sc.broadcast(queries.collect());
        JavaRDD<Set<Integer>> resultSet =
                trieRDD.map(new Function<Tuple2<Integer, Trie>, Set<Integer>>() {
                    @Override
                    public Set<Integer> call(Tuple2<Integer, Trie> v1) throws Exception {
                        List<Tuple2<Integer, Query>> localQueries = partBroadQueries.value();
                        Set<Integer> answer = new TreeSet<>();

                        for (Tuple2<Integer, Query> queryEntry : localQueries) {
//                            if (v1._2().getStartingRS() == queryEntry._2().getStartingRoadSegment()) {
                            if (v1._2().getVerticalID() == queryEntry._2().getVerticalID()) {
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
                }).filter(set -> set.isEmpty() ? false : true);

        List<Integer> collectedResult = resultSet.collect().stream().flatMap(set -> set.stream()).collect(toList());


//        List<Integer> collectedResult = resultSet.collect();
        for (Integer t : collectedResult) {
            System.out.println(t);
        }
        System.out.println("resultSetSize:" + collectedResult.size());

        sc.stop();
        sc.close();


        System.out.println("joinTime(sec):" + joinTimeAcc.value());


    }


}
