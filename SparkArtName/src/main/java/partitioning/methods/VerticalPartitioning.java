package partitioning.methods;

import comparators.IntegerComparator;
import filtering.FilterEmptyAnswers;
import filtering.ReduceNofTrajectories;
import key.selectors.CSVTrajIDSelector;
import map.functions.CSVRecToTrajME;
import map.functions.LineToCSVRec;
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
import org.spark_project.guava.collect.Iterators;
import partitioners.StartingRSPartitioner;
import projections.ProjectRoadSegments;
import scala.Tuple2;
import trie.Query;
import trie.Trie;
import utilities.CSVRecord;
import utilities.Parallelism;
import utilities.PartitioningMethods;
import utilities.Trajectory;

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
//        String fileName = "hdfs:////concatTrajectoryDataset.csv";
        String fileName= "hdfs:////synth5GBDataset.csv";
        String queryFile = "hdfs:////queryRecords.csv";
//        String queryFile = "hdfs:////queryRecordsOnesix.csv";

//        SparkConf conf = new SparkConf().setAppName(VerticalPartitioning.class.getSimpleName() + nofVerticalSlices)
//                .setMaster("local[*]")
//                .set("spark.executor.instances", "" + Parallelism.PARALLELISM);
        SparkConf conf = new SparkConf().setAppName(VerticalPartitioning.class.getSimpleName() + nofVerticalSlices);

        JavaSparkContext sc = new JavaSparkContext(conf);
        LongAccumulator queryLengthSum = sc.sc().longAccumulator("queryLengthSum");
        JavaRDD<CSVRecord> records = sc.textFile(fileName, Parallelism.PARALLELISM).map(new LineToCSVRec());
        DoubleAccumulator joinTimeAcc = sc.sc().doubleAccumulator("joinTimeAcc");

//        JavaRDD<Long> roadSegments = records.map(new ProjectRoadSegments()).sortBy(null,true,1);


        List<Integer> roadIntervals = sliceNofSegments2(records, nofVerticalSlices);

        JavaPairRDD<Integer, CSVRecord> queryRecords = sc.textFile(queryFile, Parallelism.PARALLELISM).map(new LineToCSVRec()).mapToPair(csvRec -> new Tuple2<>(csvRec.getTrajID(), csvRec));

        JavaPairRDD<Integer, Query> queries =
                queryRecords.groupByKey().mapValues(new CSVRecToTrajME()).map(t -> new Query(t._2(), roadIntervals, PartitioningMethods.VERTICAL)).mapToPair(q -> new Tuple2<>(q.getStartingRoadSegment(), q));

        JavaPairRDD<Integer, Trajectory> trajectoryDataset = records.groupBy(new CSVTrajIDSelector()).mapValues(new CSVRecToTrajME()).mapToPair(new PairFunction<Tuple2<Integer, Trajectory>, Integer, Trajectory>() {
            @Override
            public Tuple2<Integer, Trajectory> call(Tuple2<Integer, Trajectory> trajectoryTuple2) throws Exception {
//                int x = new StartingRSPartitioner(roadIntervals, nofVerticalSlices).getPartition(trajectoryTuple2._2().getStartingRS());


                return new Tuple2<Integer, Trajectory>(trajectoryTuple2._2().getStartingRS(), trajectoryTuple2._2());
            }
        }).filter(new ReduceNofTrajectories());

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


        System.out.println("Done. Trajectories build");
        JavaPairRDD<Integer, Trie> trieRDD =
                trajectoryDataset.groupByKey().flatMap(new FlatMapFunction<Tuple2<Integer, Iterable<Trajectory>>, Trie>() {
                    @Override
                    public Iterator<Trie> call(Tuple2<Integer, Iterable<Trajectory>> stringIterableTuple2) throws Exception {
                        List<Trie> trieList = new ArrayList<>();
                        Iterable<Trajectory> trajectories = stringIterableTuple2._2();
                        Trie trie = new Trie();

                        for (Trajectory traj : trajectories) {
                            trie.insertTrajectory2(traj);
//                            trie.setVerticalID(traj.getVerticalID());
                            trie.setStartingRoadSegment(traj.getStartingRS());
                        }
                        trieList.add(trie);
                        return trieList.iterator();
                    }
                }).mapToPair(new PairFunction<Trie, Integer, Trie>() {
                    @Override
                    public Tuple2<Integer, Trie> call(Trie trie) throws Exception {
                        return new Tuple2<Integer, Trie>(trie.getStartingRS(), trie);
                    }
                });

//        trieRDD.foreach(new VoidFunction<Tuple2<Integer, Trie>>() {
//            @Override
//            public void call(Tuple2<Integer, Trie> longTrieTuple2) throws Exception {
//                System.out.println("getTrajectoryCounter:" + longTrieTuple2._2().getTrajectoryCounter());
//            }
//        });
//        trajectoryDataset.map(new Function<Tuple2<Long,Trajectory>, Tuple2<Long,Trajectory>>() {
//            @Override
//            public Tuple2<Long, Trajectory> call(Tuple2<Long, Trajectory> v1) throws Exception {
//                return null;
//            }
//        })


        JavaPairRDD<Integer, Query> partitionedQueries = queries.partitionBy(new StartingRSPartitioner(roadIntervals, nofVerticalSlices)).persist(StorageLevel.MEMORY_ONLY());

        System.out.println("nofQueries:"+partitionedQueries.count());


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



        //broadcasting method

//        Broadcast<Map<Integer, Query>> partBroadQueries = sc.broadcast(queries.collectAsMap());
//        JavaRDD<Integer> resultSet = partitionedTries.mapPartitions(new FlatMapFunction<Iterator<Tuple2<Integer, Trie>>, Integer>() {
//
//            @Override
//            public Iterator<Integer> call(Iterator<Tuple2<Integer, Trie>> tuple2Iterator) throws Exception {
//                ArrayList<Tuple2<Integer, Trie>> trieList=new ArrayList<>();
//                tuple2Iterator.forEachRemaining(trieList::add);
//                Map<Integer,Query> localQueries=partBroadQueries.value();
//                Set<Map.Entry<Integer, Query>> localQEntries=localQueries.entrySet();
//                TreeMap<Integer,Trie> trieMap=new TreeMap<>();
//                ArrayList<Integer> answer=new ArrayList<>();
//                System.out.println("partition:");
//
//
//
////                for (Map.Entry<Integer, Query> queryEntry:localQEntries) {
////                    System.out.println("qid:"+queryEntry.getValue().getStartingRoadSegment());
////                }
////                for (Tuple2<Integer, Trie> t: trieList){
////
////                    System.out.println("tid:"+t._2().getStartingRS());
////                }
//                for (Map.Entry<Integer, Query> queryEntry:localQEntries) {
//
//                    for (int i = 0; i < trieList.size(); i++) {
////                        if (trieList.get(i)._1()==queryEntry.getValue().getStartingRoadSegment()){
//                        if (trieList.get(i)._2().getStartingRS()==queryEntry.getValue().getStartingRoadSegment()){
//                            answer.addAll(trieList.get(i)._2().queryIndex(queryEntry.getValue()));
//                        }
////                    }
//
//                }
//            }
//                return answer.iterator();
//        }});
        Broadcast<List<Tuple2<Integer, Query>>> partBroadQueries=sc.broadcast(queries.collect());
        JavaRDD<Set<Integer>> resultSet =
                partitionedTries.map(new Function<Tuple2<Integer,Trie>, Set<Integer>>() {
            @Override
            public Set<Integer> call(Tuple2<Integer, Trie> v1) throws Exception {
                List<Tuple2<Integer, Query>> localQueries=partBroadQueries.value();
                Set<Integer> answer=new TreeSet<>();

                for (Tuple2<Integer, Query> queryEntry:localQueries) {
                    if (v1._2().getStartingRS()==queryEntry._2().getStartingRoadSegment()) {
                        Set<Integer> ans=v1._2().queryIndex(queryEntry._2());
                        answer.addAll(ans);
                    }
                }
                return answer;
            }
        }).filter(set -> set.isEmpty()? false: true);

        List<Integer> collectedResult=resultSet.collect().stream().flatMap(set -> set.stream()).collect(toList());


//        List<Integer> collectedResult = resultSet.collect();
        for (Integer t: collectedResult) {
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