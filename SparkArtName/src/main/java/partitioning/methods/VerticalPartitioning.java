package partitioning.methods;

import comparators.IntegerComparator;
import comparators.LongComparator;
import filtering.FilterNullQueries;
import filtering.ReduceNofTrajectories2;
import key.selectors.CSVTrajIDSelector;
import key.selectors.TrajectoryTSSelector;
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

import static filtering.ReduceNofTrajectories2.MAX_TRAJECTORY_SIZE;

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
        for (int i = min; i < max ; i += interval) {
            roadIntervals.add(i);
        }
        roadIntervals.remove(roadIntervals.size()-1);
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

        System.out.println(Integer.MAX_VALUE);

        System.exit(1);

        int nofVerticalSlices = Integer.parseInt(args[0]);
//        String fileName = "file:///mnt/hgfs/VM_SHARED/samplePort.csv";
//        String fileName= "file:///mnt/hgfs/VM_SHARED/trajDatasets/85TD.csv";
//        String fileName = "file:////mnt/hgfs/VM_SHARED/trajDatasets/concatTrajectoryDataset.csv";
//          String fileName= "file:////mnt/hgfs/VM_SHARED/trajDatasets/half.csv";
//        String fileName = "file:////mnt/hgfs/VM_SHARED/trajDatasets/onesix.csv";
//          String fileName= "file:////mnt/hgfs/VM_SHARED/trajDatasets/octant.csv";
//        String fileName= "hdfs:////half.csv";
//        String fileName= "hdfs:////85TD.csv";
        String fileName = "hdfs:////concatTrajectoryDataset.csv";
//        String fileName= "hdfs:////synth5GBDataset.csv";
//        String fileName= "file:////home/giannis/octant.csv";
//        String queryFile = "file:////mnt/hgfs/VM_SHARED/trajDatasets/onesix.csv";
//        String queryFile = "file:///mnt/hgfs/VM_SHARED/samplePort.csv";
//        String queryFile = "file:///mnt/hgfs/VM_SHARED/trajDatasets/queryRecords.csv";
//        String queryFile = "file:///mnt/hgfs/VM_SHARED/trajDatasets/queryRecordsOnesix.csv";
//        String queryFile = "file:////data/queryRecords.csv";
        String queryFile = "hdfs:////queryRecords.csv";
//        String queryFile = "hdfs:////queryRecordsOnesix.csv";
//        SparkConf conf = new SparkConf().setAppName(VerticalPartitioning.class.getSimpleName() + nofVerticalSlices)
//                .setMaster("local[*]")
//                .set("spark.executor.instances", "" + Parallelism.PARALLELISM);
        SparkConf conf = new SparkConf().setAppName(VerticalPartitioning.class.getSimpleName()+nofVerticalSlices);

        JavaSparkContext sc = new JavaSparkContext(conf);
        LongAccumulator queryLengthSum = sc.sc().longAccumulator("queryLengthSum");
        JavaRDD<CSVRecord> records = sc.textFile(fileName).map(new LineToCSVRec());
        DoubleAccumulator joinTimeAcc = sc.sc().doubleAccumulator("joinTimeAcc");

//        JavaRDD<Long> roadSegments = records.map(new ProjectRoadSegments()).sortBy(null,true,1);


        List<Integer> roadIntervals = sliceNofSegments2(records, nofVerticalSlices);

        JavaPairRDD<Integer,CSVRecord> queryRecords = sc.textFile(queryFile).map(new LineToCSVRec()).mapToPair(csvRec -> new Tuple2<>(csvRec.getTrajID(), csvRec));

        JavaPairRDD<Integer, Query> queries =
                queryRecords. groupByKey().mapValues(new CSVRecToTrajME()).map(t -> new Query(t._2(), roadIntervals, PartitioningMethods.VERTICAL)).mapToPair(q -> new Tuple2<>(q.getStartingRoadSegment(), q));

        JavaPairRDD<Integer, Trajectory> trajectoryDataset = records.groupBy(new CSVTrajIDSelector()).mapValues(new CSVRecToTrajME()).mapToPair(new PairFunction<Tuple2<Integer, Trajectory>, Integer, Trajectory>() {
            @Override
            public Tuple2<Integer, Trajectory> call(Tuple2<Integer, Trajectory> trajectoryTuple2) throws Exception {
//                int x = new StartingRSPartitioner(roadIntervals, nofVerticalSlices).getPartition(trajectoryTuple2._2().getStartingRS());


                return new Tuple2<Integer, Trajectory>(trajectoryTuple2._2().getStartingRS(), trajectoryTuple2._2());
            }
        }).filter(t -> t._2().getRoadSegments().size()<MAX_TRAJECTORY_SIZE? true : false );

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




        JavaPairRDD<Integer, Query> partitionedQueries = queries.partitionBy(  new StartingRSPartitioner(roadIntervals, nofVerticalSlices)).persist(StorageLevel.MEMORY_ONLY());
        partitionedQueries.count();

        JavaPairRDD<Integer, Trie> partitionedTries = trieRDD.partitionBy(new StartingRSPartitioner(roadIntervals, nofVerticalSlices)).persist(StorageLevel.MEMORY_ONLY());
        partitionedTries.count();

//        partitionedTries.foreach(t -> System.out.println(t));
//        partitionedQueries.foreach(t -> System.out.println(t));
//        Stats.nofTriesInPartitions(trieRDD);
//        Stats.nofQueriesInEachVerticalPartition(partitionedQueries);
//        Stats.nofTriesInPartitions(partitionedTries);
//        System.out.println("nofTries::" + partitionedTries.values().collect().size());

        System.out.println("nofqueries:"+queries.count());
//        partitionedTries.cache();
//        partitionedQueries.cache();

//        JavaPairRDD<Integer, Tuple2<Trie, Query>> joinRes =partitionedTries.join(partitionedQueries);

//        joinRes.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Trie, Query>>>() {
//            @Override
//            public void call(Tuple2<Integer, Tuple2<Trie, Query>> integerTuple2Tuple2) throws Exception {
//                Tuple2<Trie, Query> trieQueryTuple2=integerTuple2Tuple2._2();
//                long t1=System.nanoTime();
//                Set<Long> result= trieQueryTuple2._1().queryIndex(trieQueryTuple2._2());
//                long t2=System.nanoTime();
//                long diff=t2-t1;
//                joinTimeAcc.add(diff*Math.pow(10.0,-9.0));;
//                System.out.println(result);
//            }
//        });
        JavaPairRDD<Integer, Set<Integer>> resultSet = partitionedTries.join(partitionedQueries).mapValues(new Function<Tuple2<Trie, Query>, Set<Integer>>() {
                    @Override
                    public Set<Integer> call(Tuple2<Trie, Query> trieQueryTuple2) throws Exception {
                        long t1=System.nanoTime();
                        Set<Integer> result= trieQueryTuple2._1().queryIndex(trieQueryTuple2._2());
                        long t2=System.nanoTime();
                        long diff=t2-t1;
                        System.out.println("queryIndexTime:"+diff);
                        joinTimeAcc.add(diff*Math.pow(10.0,-9.0));;
                        return result;
                    }
                });
//
        resultSet.foreach(new VoidFunction<Tuple2<Integer, Set<Integer>>>() {
            @Override
            public void call(Tuple2<Integer, Set<Integer>> integerSetTuple2) throws Exception {
                System.out.println(integerSetTuple2);
            }
        });

        System.out.println("resultSetSize:"+resultSet.collect().size());
        sc.stop();
        sc.close();

//        JavaPairRDD<Integer, Set<Long>> resultSet =
//                partitionedTries.join(partitionedQueries).mapValues(new Function<Tuple2<Trie, Query>, Set<Long>>() {
//                    @Override
//                    public Set<Long> call(Tuple2<Trie, Query> trieQueryTuple2) throws Exception {
//                        long t1=System.nanoTime();
//                        Set<Long> result= trieQueryTuple2._1().queryIndex(trieQueryTuple2._2());
//                        long t2=System.nanoTime();
//                        long diff=t2-t1;
//                        joinTimeAcc.add(diff*Math.pow(10.0,-9.0));;
//                        return result;
//                    }
//                }).filter(new Function<Tuple2<Integer, Set<Long>>, Boolean>() {
//                    @Override
//                    public Boolean call(Tuple2<Integer, Set<Long>> v1) throws Exception {
//                        return v1._2().isEmpty() ? false : true;
//                    }
//                });
//        resultSet.foreach(new VoidFunction<Tuple2<Integer, Set<Long>>>() {
//            @Override
//            public void call(Tuple2<Integer, Set<Long>> integerSetTuple2) throws Exception {
//                System.out.println(integerSetTuple2);
//            }
//        });

        System.out.println("joinTime(sec):"+joinTimeAcc.value());



    }


}


//broadcasting method
//        Broadcast<Map< Long,Query>> partBroadQueries=sc.broadcast(queries.collectAsMap());
//        Broadcast<JavaPairRDD<Long, Query>> partBroadQueries=sc.broadcast(queries);
//        mapPartitions(new FlatMapFunction<Iterator<Tuple2<Long, Trie>>, Long>() {
//
//            @Override
//            public Iterator<Long> call(Iterator<Tuple2<Long, Trie>> tuple2Iterator) throws Exception {
//                ArrayList<Tuple2<Long, Trie>> trieList=new ArrayList<>();
//                tuple2Iterator.forEachRemaining(trieList::add);
//                Map<Long,Query> localQueries=partBroadQueries.value();
//                Set<Map.Entry<Long, Query>> localQEntries=localQueries.entrySet();
//                TreeMap<Long,Trie> trieMap=new TreeMap<>();
//                int bull=trieList.stream().collect(Collectors.toMap(Tuple2::_1,Tuple2::_2));


//                ArrayList<Long> answer=new ArrayList<>();
//                for (Map.Entry<Long, Query> queryEntry:localQEntries) {
//
//                    for (int i = 0; i < trieList.size(); i++) {
//                        if (trieList.get(i)._1()==queryEntry.getKey()){
//                        if (trieList.get(i)._2().getStartingRS()==queryEntry.getValue().getStartingRoadSegment()){
//                            answer.addAll(trieList.get(i)._2().queryIndex(queryEntry.getValue()));
//                        }
//                    }
//
//                }
//
//
//                return answer.iterator();
//            }
//        }).take(100);