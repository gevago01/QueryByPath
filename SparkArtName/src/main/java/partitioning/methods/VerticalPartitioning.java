package partitioning.methods;

import comparators.LongComparator;
import filtering.FilterNullQueries;
import filtering.ReduceNofTrajectories2;
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
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.LongAccumulator;
import partitioners.StartingRSPartitioner;
import projections.ProjectRoadSegments;
import scala.Tuple2;
import trie.Query;
import trie.Trie;
import utilities.CSVRecord;
import utilities.Trajectory;

import java.util.*;

/**
 * Created by giannis on 10/12/18.
 */
public class VerticalPartitioning {


    private static List<Long> sliceNofSegments2(JavaRDD<CSVRecord> records, int nofVerticalSlices) {
        JavaRDD<Long> roadSegments = records.map(new ProjectRoadSegments()).distinct().sortBy(new Function<Long, Long>() {
            @Override
            public Long call(Long v1) throws Exception {
                return v1;
            }
        }, true, 1);

        long min = roadSegments.min(new LongComparator());
        long max = roadSegments.max(new LongComparator());

        long timePeriod = max - min;

        long interval = timePeriod / nofVerticalSlices;

        List<Long> roadIntervals = new ArrayList<>();
        for (long i = min; i < max ; i += interval) {
            roadIntervals.add(i);
        }
        roadIntervals.remove(roadIntervals.size()-1);
        roadIntervals.add(max);


        return roadIntervals;
    }




    private static List<Long> sliceNofSegments(JavaRDD<CSVRecord> records, int nofVerticalSlices) {
        JavaRDD<Long> roadSegments = records.map(new ProjectRoadSegments()).distinct().sortBy(new Function<Long, Long>() {
            @Override
            public Long call(Long v1) throws Exception {
                return v1;
            }
        }, true, 1);
        long nofSegments = roadSegments.count();
        JavaPairRDD<Long, Long> roadSegmentswIndex =
                roadSegments.zipWithIndex().mapToPair(new PairFunction<Tuple2<Long, Long>, Long, Long>() {
                    @Override
                    public Tuple2<Long, Long> call(Tuple2<Long, Long> t) throws Exception {
                        return new Tuple2<>(t._2(), t._1());
                    }
                });

        long bucketCapacity = nofSegments / nofVerticalSlices;

        List<Long> indices = new ArrayList<>();
        for (long i = 0; i < nofSegments - 1; i += bucketCapacity) {
            indices.add(i);
        }
        indices.add(nofSegments - 1);
        List<Long> roadIntervals = new ArrayList<>();
        for (Long l : indices) {
            roadIntervals.add(roadSegmentswIndex.lookup(l).get(0));
        }

        return roadIntervals;
    }


    public static void main(String[] args) {

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
//        String fileName= "hdfs:////onesix.csv";
//        String fileName= "hdfs:////octant.csv";
//        String fileName= "file:////home/giannis/octant.csv";
//        SparkConf conf = new SparkConf().setAppName(VerticalPartitioning.class.getSimpleName() + nofVerticalSlices)
//                .setMaster("local[*]")
//                .set("spark.executor.instances", "" + Parallelism.PARALLELISM);
        SparkConf conf = new SparkConf().setAppName(VerticalPartitioning.class.getSimpleName()+nofVerticalSlices);

        JavaSparkContext sc = new JavaSparkContext(conf);
        LongAccumulator queryLengthSum = sc.sc().longAccumulator("queryLengthSum");
        JavaRDD<CSVRecord> records = sc.textFile(fileName).map(new LineToCSVRec());
        DoubleAccumulator joinTimeAcc = sc.sc().doubleAccumulator("joinTimeAcc");

//        JavaRDD<Long> roadSegments = records.map(new ProjectRoadSegments()).sortBy(null,true,1);


        List<Long> roadIntervals = sliceNofSegments(records, nofVerticalSlices);


        JavaPairRDD<Integer, Trajectory> trajectoryDataset = records.groupBy(new CSVTrajIDSelector()).mapValues(new CSVRecToTrajME()).mapToPair(new PairFunction<Tuple2<Long, Trajectory>, Integer, Trajectory>() {
            @Override
            public Tuple2<Integer, Trajectory> call(Tuple2<Long, Trajectory> trajectoryTuple2) throws Exception {
                int x = new StartingRSPartitioner(roadIntervals, nofVerticalSlices).getPartition(trajectoryTuple2._2().getStartingRS());
                trajectoryTuple2._2().setVerticalID(x);
                return new Tuple2<Integer, Trajectory>(x, trajectoryTuple2._2());
            }
        }).filter(new ReduceNofTrajectories2());

//        trajectoryDataset.count();
        System.out.println("TrieHistogram:"+StartingRSPartitioner.histogram);
        Set<Map.Entry<Integer, Integer>> histogramValues = StartingRSPartitioner.histogram.entrySet();

        for (Map.Entry<Integer, Integer> entry:histogramValues) {

            System.out.println(entry.getKey()+","+entry.getValue());
        }
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
                            trie.setVerticalID(traj.getVerticalID());
                        }
                        trieList.add(trie);
                        return trieList.iterator();
                    }
                }).mapToPair(new PairFunction<Trie, Integer, Trie>() {
                    @Override
                    public Tuple2<Integer, Trie> call(Trie trie) throws Exception {
                        return new Tuple2<>(trie.getVerticalID(), trie);
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
        HashMap<Integer,Integer> queryHistogram = new HashMap<>();
        JavaPairRDD<Integer, Query> queries =
                trajectoryDataset.values().map(new Function<Trajectory, Query>() {

                    private Random random = new Random();


                    private final static float PROBABILITY = 0.0001f;
//                    private final static float PROBABILITY = 1f;

                    private boolean getRandomBoolean(float p) {

                        return random.nextFloat() < p;
                    }

                    @Override
                    public Query call(Trajectory trajectory) throws Exception {
                        if (getRandomBoolean(PROBABILITY)) {
                            Query q = new Query(trajectory.getStartingTime(), trajectory.getEndingTime(), trajectory.roadSegments);
                            q.setVerticalID(trajectory.getVerticalID());
                            queryLengthSum.add(trajectory.roadSegments.size());
                            return q;
                        }

                        return null;
                    }
                }).filter(new FilterNullQueries()).mapToPair(new PairFunction<Query, Integer, Query>() {
                    @Override
                    public Tuple2<Integer, Query> call(Query query) throws Exception {
                        Integer val = queryHistogram.get(query.getVerticalID());
                        if (val==null){
                            queryHistogram.put(query.getVerticalID(),0);
                        }
                        else{
                            queryHistogram.put(query.getVerticalID(),val+1);

                        }
                        return new Tuple2<>(query.getVerticalID(), query);
                    }
                });


        List<Integer> queryKeys=queries.keys().collect();

        for (Integer i:queryKeys) {
            Integer val = queryHistogram.get(i);
            if (val==null){
                queryHistogram.put(i,0);
            }
            else{
                queryHistogram.put(i,val+1);

            }
        }
//        queries.count();

        System.out.println("queryHistogram:"+queryHistogram);


        JavaPairRDD<Integer, Query> partitionedQueries = queries.partitionBy(new StartingRSPartitioner(roadIntervals, nofVerticalSlices)).persist(StorageLevel.MEMORY_ONLY());
        JavaPairRDD<Integer, Trie> partitionedTries = trieRDD.partitionBy(new StartingRSPartitioner(roadIntervals, nofVerticalSlices)).persist(StorageLevel.MEMORY_ONLY());
//        partitionedTries.foreach(t -> System.out.println(t));
//        partitionedQueries.foreach(t -> System.out.println(t));
//        Stats.nofTriesInPartitions(trieRDD);
//        Stats.nofQueriesInEachVerticalPartition(partitionedQueries);
//        Stats.nofTriesInPartitions(partitionedTries);
//        System.out.println("nofTries::" + partitionedTries.values().collect().size());

        System.out.println("nofqueries:"+queries.count());
//        partitionedTries.cache();
//        partitionedQueries.cache();

        JavaPairRDD<Integer, Tuple2<Trie, Query>> joinRes =partitionedTries.join(partitionedQueries);

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
        JavaPairRDD<Integer, Set<Long>> resultSet = joinRes.mapValues(new Function<Tuple2<Trie, Query>, Set<Long>>() {
                    @Override
                    public Set<Long> call(Tuple2<Trie, Query> trieQueryTuple2) throws Exception {
                        long t1=System.nanoTime();
                        Set<Long> result= trieQueryTuple2._1().queryIndex(trieQueryTuple2._2());
                        long t2=System.nanoTime();
                        long diff=t2-t1;
                        joinTimeAcc.add(diff*Math.pow(10.0,-9.0));;
                        return result;
                    }
                });
//
        resultSet.foreach(new VoidFunction<Tuple2<Integer, Set<Long>>>() {
            @Override
            public void call(Tuple2<Integer, Set<Long>> integerSetTuple2) throws Exception {
                System.out.println(integerSetTuple2);
            }
        });

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