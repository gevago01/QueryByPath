package partitioning.methods;

import map.functions.AddTrajToTrie;
import map.functions.CSVRecToTrajME;
import map.functions.LineToCSVRec;
import map.functions.VerticalQueryMap;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.DoubleAccumulator;
import scala.Tuple2;
import trie.Query;
import trie.Trie;
import utilities.*;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;


/**
 * Created by giannis on 10/12/18.
 */
public class VerticalPartitioning {

    public static void main(String[] args) {


        String dataHdfsName = null;
        String queryHdfsName = null;
        int bucketCapacity = 0;

        if (args.length == 2) {
            dataHdfsName = args[0];
            queryHdfsName = args[1];
        } else if (args.length == 3) {
            dataHdfsName = args[0];
            queryHdfsName = args[1];
            bucketCapacity = Integer.parseInt(args[2]);
        }
//        String fileName = "hdfs:////" + dataHdfsName;
//        String queryFile = "hdfs:////" + queryHdfsName;
        String queryFile = "file:////mnt/hgfs/VM_SHARED/trajDatasets/onesix.csv";
        String fileName = "file:////mnt/hgfs/VM_SHARED/trajDatasets/onesix.csv";
//        String fileName = "file:///mnt/hgfs/VM_SHARED/samplePort.csv";

//        String queryFile = "file:///mnt/hgfs/VM_SHARED/trajDatasets/queryRecords.csv";
//        String queryFile = "file:////data/queryRecords.csv";

        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .set("spark.executor.instances", "" + Parallelism.PARALLELISM)
                .setAppName(VerticalPartitioning.class.getSimpleName());
//        SparkConf conf = new SparkConf().setAppName(VerticalPartitioning.class.getSimpleName() + bucketCapacity);

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<CSVRecord> records = sc.textFile(fileName).map(new LineToCSVRec());
        DoubleAccumulator joinTimeAcc = sc.sc().doubleAccumulator("joinTimeAcc");

        Integer skewnessFactor;
        try {
            String skewness = dataHdfsName.substring(0, 2);
            skewnessFactor = Integer.parseInt(skewness);
        } catch (Exception e) {
            skewnessFactor = 1;

        }
        HybridConfiguration.configure(records, skewnessFactor);
        if (args.length != 3) {
            bucketCapacity = HybridConfiguration.getBucketCapacityLowerBound();
        }


        JavaPairRDD<Integer, CSVRecord> queryRecords = sc.textFile(queryFile, Parallelism.PARALLELISM).map(new LineToCSVRec()).mapToPair(csvRec -> new Tuple2<>(csvRec.getTrajID(), csvRec));


        JavaPairRDD<Integer, Trajectory> trajectoryDataset = records.groupBy(csv -> csv.getTrajID()).mapValues(new CSVRecToTrajME());

        int nofTrajs = (int) trajectoryDataset.count();
        if (bucketCapacity > nofTrajs) {
            System.err.println("bucketCapacity>nofTrajs");
            System.exit(1);
        }


//        long nofUniqueSegments = records.map(r -> r.getRoadSegment()).distinct().count();
//
//        long minTimestamp = records.map(r -> r.getTimestamp()).min(new LongComparator());
//        long maxTimestamp = records.map(r -> r.getTimestamp()).max(new LongComparator());
//        int minRS = records.map(r -> r.getRoadSegment()).min(new IntegerComparator());
//        int maxRS = records.map(r -> r.getRoadSegment()).max(new IntegerComparator());
//


        Map<Integer, Integer> buckets = Intervals.sliceRSToBuckets2(trajectoryDataset, bucketCapacity);

        trajectoryDataset = trajectoryDataset.mapToPair(trajectory -> {
            Integer bucket = buckets.get(trajectory._2().getTrajectoryID());

            trajectory._2().setPartitionID(bucket);
            return new Tuple2<>(trajectory._2().getPartitionID(), trajectory._2());
        });

        JavaPairRDD<Integer, Query> queries =
                queryRecords.groupByKey().mapValues(new CSVRecToTrajME()).map(t -> new Query(t._2())).mapToPair(q -> {
//                    q.setPartitionID(buckets.get(q.getStartingRoadSegment()));
                    return new Tuple2<>(q.getPartitionID(), q);
                });
//        filter(q -> buckets.get(q.getStartingRoadSegment())==null?false:true ).


//        JavaPairRDD<Integer, Trajectory>trajectoryDataset.mapToPair(trajectory -> new Tuple2<>(trajectory._2().getPartitionID(),trajectory));


//        Stats.nofTrajsInEachSlice(trajectoryDataset, PartitioningMethods.VERTICAL);
//        Stats.nofQueriesInSlice(queries, PartitioningMethods.VERTICAL);
        System.out.println("Done. Trajectories build");


        int nofPartitions = Math.toIntExact(buckets.values().stream().distinct().count());
        System.out.println("nofBuckets:" + nofPartitions);
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

//        Stats.nofQueriesOnEachNode(queries, PartitioningMethods.VERTICAL);

        JavaPairRDD<Integer, Trie> trieRDD = emptyTrieRDD.groupWith(trajectoryDataset).mapValues(new AddTrajToTrie());
        long noftries = trieRDD.count();
        System.out.println("nofTries:" + noftries);


        Broadcast<List<Tuple2<Integer, Query>>> partBroadQueries = sc.broadcast(queries.collect());
        JavaRDD<Set<Integer>> resultSet =
                trieRDD.map(new VerticalQueryMap(partBroadQueries, joinTimeAcc)).filter(set -> set.isEmpty() ? false : true);

        List<Integer> collectedResult = resultSet.collect().stream().flatMap(set -> set.stream()).distinct().collect(toList());

        for (Integer t : collectedResult) {
            System.out.println(t);
        }
        System.out.println("resultSetSize:" + collectedResult.size());

        sc.stop();
        sc.close();

        joinTimeAcc.setValue(joinTimeAcc.value());
        System.out.println("joinTime(sec):" + joinTimeAcc.value());

    }


}
