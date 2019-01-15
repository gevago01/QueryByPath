package partitioning.methods;

import filtering.FilterNullQueries;
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
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.LongAccumulator;
import partitioners.LongPartitioner;
import partitioners.StartingRSPartitioner;
import projections.ProjectRoadSegments;
import scala.Tuple2;
import trie.Query;
import trie.Trie;
import utilities.CSVRecord;
import utilities.Parallelism;
import utilities.Trajectory;

import java.util.*;

/**
 * Created by giannis on 10/12/18.
 */
public class VerticalPartitioning {


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
                        return new Tuple2<>(t._2(),t._1());
                    }
                });

        roadSegmentswIndex.foreach(new VoidFunction<Tuple2<Long, Long>>() {
            @Override
            public void call(Tuple2<Long, Long> longLongTuple2) throws Exception {
                System.out.println(longLongTuple2);
            }
        });
        long interval = nofSegments / nofVerticalSlices;

        List<Long> indices = new ArrayList<>();
        for (long i = 0; i < nofSegments; i += interval) {
            indices.add(i);
        }
        indices.add(nofSegments-1);
        List<Long> roadIntervals = new ArrayList<>();
        for (Long l:indices) {
            roadIntervals.add(roadSegmentswIndex.lookup(l).get(0));
        }
        System.out.println("roadIntervals:"+roadIntervals);

        return roadIntervals;
    }


    public static void main(String[] args) {
        int nofVerticalSlices = Integer.parseInt(args[0]);
//        String fileName = "file:///mnt/hgfs/VM_SHARED/samplePort.csv";
//        String fileName= "file:///mnt/hgfs/VM_SHARED/trajDatasets/85TD.csv";
//        String fileName = "file:////mnt/hgfs/VM_SHARED/trajDatasets/concatTrajectoryDataset.csv";
//          String fileName= "file:////mnt/hgfs/VM_SHARED/trajDatasets/half.csv";
        String fileName = "file:////mnt/hgfs/VM_SHARED/trajDatasets/onesix.csv";
//          String fileName= "file:////mnt/hgfs/VM_SHARED/trajDatasets/octant.csv";
//        String fileName= "hdfs:////half.csv";
//        String fileName= "hdfs:////85TD.csv";
//        String fileName = "hdfs:////concatTrajectoryDataset.csv";
//        String fileName= "hdfs:////onesix.csv";
//        String fileName= "hdfs:////octant.csv";
//        String fileName= "file:////home/giannis/octant.csv";
        SparkConf conf = new SparkConf().setAppName(VerticalPartitioning.class.getSimpleName())
                .setMaster("local[*]")
                .set("spark.executor.instances", "" + Parallelism.PARALLELISM);
//        SparkConf conf = new SparkConf().setAppName(VerticalPartitioning.class.getSimpleName())
//                .set("spark.executor.instances", "" + Parallelism.PARALLELISM)
//                .set("spark.executor.cores", "" + Parallelism.EXECUTOR_CORES);//.set("spark.executor.heartbeatInterval","15s");
        JavaSparkContext sc = new JavaSparkContext(conf);
        LongAccumulator nofQueries = sc.sc().longAccumulator("NofQueries");
        LongAccumulator queryLengthSum = sc.sc().longAccumulator("queryLengthSum");
        JavaRDD<CSVRecord> records = sc.textFile(fileName).map(new LineToCSVRec());

//        JavaRDD<Long> roadSegments = records.map(new ProjectRoadSegments()).sortBy(null,true,1);


        List<Long> roadIntervals=sliceNofSegments(records, nofVerticalSlices);

        JavaPairRDD<Long, Trajectory> trajectoryDataset = records.groupBy(new CSVTrajIDSelector()).mapValues(new CSVRecToTrajME()).mapToPair(new PairFunction<Tuple2<Long, Trajectory>, Long, Trajectory>() {
            @Override
            public Tuple2<Long, Trajectory> call(Tuple2<Long, Trajectory> trajectoryTuple2) throws Exception {
                return new Tuple2<>(trajectoryTuple2._2().getStartingRS(), trajectoryTuple2._2());
            }
        }).filter(new ReduceNofTrajectories());
        long x = trajectoryDataset.count();

        System.out.println("nof trajs<100:" + x);

//        System.exit(1);
//        trajectoryDataset.aggregateByKey()
//        JavaPairRDD<Long, Trie> trieRDD = trajectoryDataset.combineByKey(new TrajectoryCombiner(), new MergeTrajectory(),new MergeTries(), new LongPartitioner()).mapToPair(new PairFunction<Tuple2<Long,Trie>, Long, Trie>() {
//            @Override
//            public Tuple2<Long, Trie> call(Tuple2<Long, Trie> longTrieTuple2) throws Exception {
//                longTrieTuple2._2().insertAllTrajs();
//                return longTrieTuple2;
//            }
//        });


        System.out.println("Done. Trajectories build");
        JavaPairRDD<Long, Trie> trieRDD =
                trajectoryDataset.groupByKey().flatMap(new FlatMapFunction<Tuple2<Long, Iterable<Trajectory>>, Trie>() {
                    @Override
                    public Iterator<Trie> call(Tuple2<Long, Iterable<Trajectory>> stringIterableTuple2) throws Exception {
                        List<Trie> trieList = new ArrayList<>();
                        Iterable<Trajectory> trajectories = stringIterableTuple2._2();
                        Trie trie = new Trie();
                        for (Trajectory traj : trajectories) {
                            trie.insertTrajectory2(traj.roadSegments, traj.trajectoryID, traj.getStartingTime(), traj.getEndingTime());
                        }
                        trieList.add(trie);
                        return trieList.iterator();
                    }
                }).mapToPair(new PairFunction<Trie, Long, Trie>() {
                    @Override
                    public Tuple2<Long, Trie> call(Trie trie) throws Exception {
                        return new Tuple2<>(trie.getStartingRS(), trie);
                    }
                });

        JavaPairRDD<Long, Query> queries =
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
                            Query q = new Query(trajectory.getStartingTime(), trajectory.getEndingTime(), trajectory.roadSegments);
                            nofQueries.add(1L);
                            queryLengthSum.add(trajectory.roadSegments.size());
                            return q;
                        }
                        nofQueries.add(1L);

                        return null;
                    }
                }).filter(new FilterNullQueries()).mapToPair(new PairFunction<Query, Long, Query>() {
                    @Override
                    public Tuple2<Long, Query> call(Query query) throws Exception {
                        return new Tuple2<>(query.getStartingRoadSegment(), query);
                    }
                });

        System.out.println("nofQueries:" + queries.count());

//        JavaPairRDD<Long, Query> partitionedQueries = queries.partitionBy(new LongPartitioner()).persist(StorageLevel.MEMORY_ONLY());
//        JavaPairRDD<Long, Trie> partitionedTries = trieRDD.partitionBy(new LongPartitioner()).persist(StorageLevel.MEMORY_ONLY());

        JavaPairRDD<Long, Query> partitionedQueries = queries.partitionBy(new StartingRSPartitioner(roadIntervals)).persist(StorageLevel.MEMORY_ONLY());
        JavaPairRDD<Long, Trie> partitionedTries = trieRDD.partitionBy(new StartingRSPartitioner(roadIntervals)).persist(StorageLevel.MEMORY_ONLY());

        JavaPairRDD<Long, Set<Long>> resultSet = partitionedTries.join(partitionedQueries).mapValues(new Function<Tuple2<Trie, Query>, Set<Long>>() {
            @Override
            public Set<Long> call(Tuple2<Trie, Query> trieQueryTuple2) throws Exception {
                return trieQueryTuple2._1().queryIndex(trieQueryTuple2._2());
            }
        }).filter(new Function<Tuple2<Long, Set<Long>>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Long, Set<Long>> v1) throws Exception {
                return v1._2().isEmpty() ? false : true;
            }
        });


        List<Tuple2<Long, Set<Long>>> lisof = resultSet.collect();
        for (Tuple2<Long, Set<Long>> t : lisof) {
            System.out.println(t);
        }
        System.out.println("resultSetSize:" + lisof.size());

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