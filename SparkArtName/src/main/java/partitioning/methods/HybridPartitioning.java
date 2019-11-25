package partitioning.methods;

import map.functions.AddTrajToTrie;
import map.functions.CSVRecToTrajME;
import map.functions.LineToCSVRec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.DoubleAccumulator;
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






    public static void main(String[] args) {


//        int slices = Integer.parseInt(args[1]);

        String dataHdfsName = null;
        String queryHdfsName = null;
        int bucketCapacity = 0;
        if (args.length == 2) {
            dataHdfsName = args[0];
            queryHdfsName = args[1];
        }
        else if (args.length ==3) {
            dataHdfsName = args[0];
            queryHdfsName = args[1];
            bucketCapacity = Integer.parseInt(args[2]);
        }






        String fileName = "hdfs:////"+dataHdfsName;
        String queryFile = "hdfs:////"+queryHdfsName;
//        String fileName = "file:///mnt/hgfs/VM_SHARED/trajDatasets/onesix.csv";
//        String queryFile = "file:///mnt/hgfs/VM_SHARED/trajDatasets/onesix.csv";
//        String fileName = "file:////mnt/hgfs/VM_SHARED/trajDatasets/lengthSkewedDataset/80pcLS.csv";
//        String queryFile = "file:////mnt/hgfs/VM_SHARED/trajDatasets/lengthSkewedDataset/80pcLSQueries.csv";
//        String fileName = "file:///mnt/hgfs/VM_SHARED/trajDatasets/rssd.csv";
//        String queryFile = "file:///mnt/hgfs/VM_SHARED/trajDatasets/concatTrajectoryQueries";
//        SparkConf conf = new SparkConf().setMaster("local[*]")
//                .set("spark.executor.instances", "" + Parallelism.PARALLELISM)
//                .set("spark.driver.maxResultSize", "3G")
//                .setAppName(HybridPartitioning.class.getSimpleName());
        SparkConf conf = new SparkConf().setAppName(HybridPartitioning.class.getSimpleName()+bucketCapacity);



        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<CSVRecord> records = sc.textFile(fileName).map(new LineToCSVRec());

        Integer skewnessFactor;
        try {
            String skewness = dataHdfsName.substring(0, 2);
            skewnessFactor = Integer.parseInt(skewness);
        }
        catch (Exception e){
            skewnessFactor = 1;
        }
        HybridConfiguration.configure(records, skewnessFactor);
        if (args.length!=3) {
            bucketCapacity = HybridConfiguration.getBucketCapacityLowerBound();
        }

//        int bucketCapacity = HybridConfiguration.getBucketCapacityLowerBound();
//        int bucketCapacity = Integer.parseInt(args[2]);
        String appName = HorizontalPartitioning.class.getSimpleName() ;
        conf.setAppName(appName);


        HybridPartitioner hp=new HybridPartitioner(records, HybridConfiguration.getRt_upperBound(),HybridConfiguration.getRl_upperBound(), HybridConfiguration.getRs_upperBound());


        DoubleAccumulator joinTimeAcc = sc.sc().doubleAccumulator("joinTimeAcc");


        JavaPairRDD<Integer, Trajectory> trajectoryDataset = records.groupBy(csv -> csv.getTrajID()).mapValues(new CSVRecToTrajME()).mapToPair((PairFunction<Tuple2<Integer, Trajectory>, Integer, Trajectory>) tr -> new Tuple2<>(hp.determineSlice(tr._2()), tr._2()));


        int nofTrajs = (int) trajectoryDataset.count();
        if (bucketCapacity>nofTrajs){
            System.err.println("bucketCapacity>nofTrajs");
            System.exit(1);
        }

//        trajectoryDataset = trajectoryDataset.mapToPair(trajectory -> {
//
//            Integer bucket = HybridPartitioning.determineSlice(trajectory._2());
//
//            trajectory._2().setPartitionID(bucket);
//            return new Tuple2<>(trajectory._2().getPartitionID(), trajectory._2());
//        });
//        /**********************hybrid2 approach**********************/
        HashMap<Integer, Integer> buckets = Intervals.hybridSlicing(trajectoryDataset, bucketCapacity);

        trajectoryDataset = trajectoryDataset.mapToPair(trajectory -> {
            Integer bucket = buckets.get(trajectory._2().getTrajectoryID());

            trajectory._2().setPartitionID(bucket);
            return new Tuple2<>(trajectory._2().getPartitionID(), trajectory._2());
        });

        int nofPartitions = Math.toIntExact(buckets.values().stream().distinct().count());
//        /**********************hybrid2 approach**********************/


        //uncomment this for hybrid1
        List<Integer> partitions = IntStream.range(0, nofPartitions).boxed().collect(toList());

//        List<Long> partitions = trajectoryDataset2.keys().distinct().collect();

//        JavaRDD<Long> partitionsRDD = trajectoryDataset2.keys().distinct();
        JavaRDD<Integer> partitionsRDD = sc.parallelize(partitions);
        JavaPairRDD<Integer, Trie> emptyTrieRDD = partitionsRDD.mapToPair(new PairFunction<Integer, Integer, Trie>() {
            @Override
            public Tuple2<Integer, Trie> call(Integer partitionID) throws Exception {
                Trie t = new Trie();
                t.setPartitionID(partitionID);
                return new Tuple2<>(partitionID, t);
            }
        });


        JavaPairRDD<Integer, Trie> trieRDD = emptyTrieRDD.groupWith(trajectoryDataset).mapValues(new AddTrajToTrie());
        long noftries = trieRDD.count();
        System.out.println("nofTries:" + noftries);



        JavaPairRDD<Integer, CSVRecord> queryRecords = sc.textFile(queryFile).map(new LineToCSVRec()).mapToPair(csvRec -> new Tuple2<>(csvRec.getTrajID(), csvRec));
        JavaRDD< Query> queries =
                queryRecords.groupByKey().mapValues(new CSVRecToTrajME()).map(t -> new Query(t._2()));

        Broadcast<List< Query>> partBroadQueries = sc.broadcast(queries.collect());


        JavaRDD<Set<Integer>> resultSetRDD =
                trieRDD.map(new Function<Tuple2<Integer, Trie>, Set<Integer>>() {


                    @Override
                    public Set<Integer> call(Tuple2<Integer, Trie> v1) throws Exception {
                        Set<Integer> answer = new TreeSet<>();

                        List<Query> localQueries = partBroadQueries.value();

                        for (Query queryEntry : localQueries) {

//                            int slice = determineQuerySlice(queryEntry._2());
//                            if (queryEntry._1() == v1._2().getPartitionID()) {
                            Set<Integer> ans=null;
                            long t1 = System.nanoTime();
                            if (queryEntry.getStartingTime() >= v1._2().getMinStartingTime() && queryEntry.getStartingTime() <= v1._2().getMaxStartingTime()) {
                                if (queryEntry.getStartingRoadSegment() >= v1._2().getMinStartingRS() && queryEntry.getStartingRoadSegment() <= v1._2().getMaxStartingRS()) {
//                                    if (queryEntry.getPathSegments().size() >= v1._2().getMinTrajLength() && queryEntry.getPathSegments().size() <= v1._2().getMaxTrajLength()) {
                                        ans = v1._2().queryIndex(queryEntry);
                                    }
                                }
//                            }
                            long t2 = System.nanoTime();
                            if (ans!=null) {
                                answer.addAll(ans);
                            }
                            long diff = t2 - t1;
                            joinTimeAcc.add(diff * Math.pow(10.0, -9.0));

                        }
                        return answer;
                    }
                }).filter(set -> set.isEmpty() ? false : true);
        List<Integer> collectedResult = resultSetRDD.collect().stream().flatMap(set -> set.stream()).distinct().collect(toList());

        for (Integer t : collectedResult) {
            System.out.println(t);
        }

        joinTimeAcc.setValue(joinTimeAcc.value());
        System.out.println("resultSetSize:" + collectedResult.size());
        System.out.println("joinTime(sec):" + joinTimeAcc.value());


    }


}