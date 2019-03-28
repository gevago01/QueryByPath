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

import java.util.*;

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
//        String fileName= "file:///mnt/hgfs/VM_SHARED/trajDatasets/85TD.csv";
//        String fileName = "file:////mnt/hgfs/VM_SHARED/trajDatasets/concatTrajectoryDataset.csv";
//          String fileName= "file:////mnt/hgfs/VM_SHARED/trajDatasets/half.csv";
//          String fileName= "file:////mnt/hgfs/VM_SHARED/trajDatasets/octant.csv";
//        String fileName= "hdfs:////half.csv";
//        String fileName= "hdfs:////85TD.csv";


//        String fileName= "file:////home/giannis/octant.csv";
//        String queryFile = "file:////mnt/hgfs/VM_SHARED/trajDatasets/onesix.csv";
//        String fileName = "file:////mnt/hgfs/VM_SHARED/trajDatasets/onesix.csv";
//        String fileName = "file:///mnt/hgfs/VM_SHARED/samplePort.csv";
//        String queryFile = "file:///mnt/hgfs/VM_SHARED/samplePort.csv";;
//        String queryFile = "file:///mnt/hgfs/VM_SHARED/samplePort.csv";
//        String queryFile = "file:///mnt/hgfs/VM_SHARED/trajDatasets/queryRecords.csv";
//        String queryFile = "file:///mnt/hgfs/VM_SHARED/trajDatasets/queryRecordsOnesix.csv";
//        String queryFile = "file:////data/queryRecords.csv";
        String fileName = "hdfs:////concatTrajectoryDataset.csv";
//        String fileName= "hdfs:////synth5GBDataset.csv";
//        String queryFile = "hdfs:////queryRecords.csv";
//        String queryFile = "hdfs:////200KqueryRecords.csv";
//        String queryFile = "hdfs:////queryRecordsOnesix.csv";
        String queryFile = "hdfs:////200KqueryRecords.csv";
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

        queries.count();
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

//



        System.out.println("Done. Trajectories build");

        JavaPairRDD<Integer, Trie> trieRDD =
                trajectoryDataset.groupByKey().mapValues(new Function<Iterable<Trajectory>, Trie>() {
                    @Override
                    public Trie call(Iterable<Trajectory> trajectories) throws Exception {
                        Trie trie = new Trie();

                        for (Trajectory traj : trajectories) {
                            trie.insertTrajectory2(traj);
                            trie.setVerticalID(traj.getVerticalID());
//                            trie.setStartingRoadSegment(traj.getStartingRS());
                        }
                        return trie;
                    }
                });

//        JavaPairRDD<Integer, Trie> trieRDD =
//                trajectoryDataset.groupByKey().flatMapValues(new Function<Iterable<Trajectory>, Iterable<Trie>>() {
//                    @Override
//                    public Iterable<Trie> call(Iterable<Trajectory> trajectories) throws Exception {
//                        List<Trie> trieList = new ArrayList<>();
//                        Trie trie = new Trie();
//
//                        for (Trajectory traj : trajectories) {
//                            trie.insertTrajectory2(traj);
//                            trie.setVerticalID(traj.getVerticalID());
////                            trie.setStartingRoadSegment(traj.getStartingRS());
//                        }
//                        trieList.add(trie);
//                        return trieList;
//                    }
//                });


        JavaPairRDD<Integer, Trie> partitionedTries = trieRDD.partitionBy(new StartingRSPartitioner(roadIntervals, nofVerticalSlices)).persist(StorageLevel.MEMORY_ONLY());
        System.out.println("nofTries:" + partitionedTries.count());


        System.out.println("TrieNofPartitions:" + partitionedTries.rdd().partitions().length);

//        partitionedTries.foreach(t -> System.out.println(t));
//        partitionedQueries.foreach(t -> System.out.println(t));
//        Stats.nofTriesInPartitions(trieRDD);
//        Stats.nofQueriesInEachVerticalPartition(partitionedQueries);
//        Stats.nofTriesInPartitions(partitionedTries);
//        System.out.println("nofTries::" + partitionedTries.values().collect().size());


//        partitionedTries.cache();
//        partitionedQueries.cache();


//
//        List<Set<Integer>> collectedResult = resultSet.values().collect();
//        collectedResult.stream().forEach(System.out::println);
//        System.out.println("resultSetSize:" + collectedResult.size());



        Broadcast<List<Tuple2<Integer, Query>>> partBroadQueries = sc.broadcast(queries.collect());
        JavaRDD<Set<Integer>> resultSet =
                partitionedTries.map(new Function<Tuple2<Integer, Trie>, Set<Integer>>() {
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


//        JavaPairRDD<Integer, Set<Integer>> resultSet = partitionedTries.join(partitionedQueries).mapValues(new Function<Tuple2<Trie, Query>, Set<Integer>>() {
//            @Override
//            public Set<Integer> call(Tuple2<Trie, Query> trieQueryTuple2) throws Exception {
////                long t1 = System.nanoTime();
//                Set<Integer> result = trieQueryTuple2._1().queryIndex(trieQueryTuple2._2());
////                long t2 = System.nanoTime();
////                long diff = t2 - t1;
////                System.out.println("queryIndexTime:" + diff);
////                joinTimeAcc.add(diff * Math.pow(10.0, -9.0));
//                return result;
//            }
//        });



//        JavaPairRDD<Integer, Trie> trieRDD =trajectoryDataset.repartitionAndSortWithinPartitions(new Partitioner() {
//            @Override
//            public int getPartition(Object key) {
//                Integer partKey = (Integer)key;
//                return partKey;
//            }
//
//            @Override
//            public int numPartitions() {
//                return 1;
//            }
//        }).mapPartitions(new FlatMapFunction<Iterator<Tuple2<Integer,Trajectory>>, Trie>() {
//            @Override
//            public Iterator<Trie> call(Iterator<Tuple2<Integer, Trajectory>> tuple2Iterator) throws Exception {
//
//                List<Trie> trieList = new ArrayList<>();
//                Trie trie = new Trie();
//
//                for (Iterator<Tuple2<Integer, Trajectory>> it = tuple2Iterator; it.hasNext(); ) {
//                    Tuple2<Integer, Trajectory> sd = it.next();
//
//                    trie.insertTrajectory2(sd._2());
//                    trie.setStartingRoadSegment(sd._2().getStartingRS());
//                }
//                trieList.add(trie);
//                return trieList.iterator();
//            }
//        }).mapToPair(new PairFunction<Trie, Integer, Trie>() {
//            @Override
//            public Tuple2<Integer, Trie> call(Trie trie) throws Exception {
//                return new Tuple2<Integer, Trie>(trie.getStartingRS(), trie);
//            }
//        });

//        trajectoryDataset.count();
//                forEach(a -> System.out.println(a));
//        trajectoryDataset.aggregateByKey()
//        JavaPairRDD<Long, Trie> trieRDD = trajectoryDataset.combineByKey(new TrajectoryCombiner(), new MergeTrajectory(),new MergeTries(), new LongPartitioner()).mapToPair(new PairFunction<Tuple2<Long,Trie>, Long, Trie>() {
//            @Override
//            public Tuple2<Long, Trie> call(Tuple2<Long, Trie> longTrieTuple2) throws Exception {
//                longTrieTuple2._2().insertAllTrajs();
//                return longTrieTuple2;
//            }
//        });
