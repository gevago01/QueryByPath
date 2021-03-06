package utilities;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;
import trie.Query;
import trie.Trie;

import java.util.*;

/**
 * Created by giannis on 14/01/19.
 */
public class Stats {
//    public static int maxQueryLength(List<Query> queryList) {
//        int max = Integer.MIN_VALUE;
//
//        for (Query q : queryList) {
//            if (q.getPathSegments().size() > max) {
//                max = q.getPathSegments().size();
//            }
//        }
//        return max;
//    }
//    public static int minQueryLength(List<Query> queryList) {
//        int min = Integer.MAX_VALUE;
//
//        for (Query q : queryList) {
//            if (q.getPathSegments().size() < min) {
//                min = q.getPathSegments().size();
//            }
//        }
//        return min;
//    }
//    public static double avgQueryLength(List<Query> queryList) {
//        int sum = 0;
//
//        for (Query q : queryList) {
//            sum += q.getPathSegments().size();
//        }
//        return sum / (double) queryList.size();
//    }
//    public static void printStats(List<Query> queryList) {
//        double avgQLength = avgQueryLength(queryList);
//        double minQLength = minQueryLength(queryList);
//        double maxQLength = maxQueryLength(queryList);
//
//        System.out.println("avgQLength:" + avgQLength);
//        System.out.println("minQLength:" + minQLength);
//        System.out.println("maxQLength:" + maxQLength);
//    }

    public static void printStats(List<Integer> lengthList) {

        double avgQLength = avgQueryLength(lengthList);
        double minQLength = minQueryLength(lengthList);
        double maxQLength = maxQueryLength(lengthList);

        System.out.println("avgQLength:" + avgQLength);
        System.out.println("minQLength:" + minQLength);
        System.out.println("maxQLength:" + maxQLength);
    }

    private static Integer maxQueryLength(List<Integer> lengthList) {
        return lengthList.stream().mapToInt(v -> v).max().getAsInt();
    }

    private static Integer minQueryLength(List<Integer> lengthList) {
        return lengthList.stream().mapToInt(v -> v).min().getAsInt();
    }

    private static double avgQueryLength(List<Integer> lengthList) {
        return lengthList.stream().mapToDouble(v -> v).average().getAsDouble();
    }

    public static void nofTriesInPartitions(final JavaPairRDD<Integer, Trie> partitionedTries) {
        List<Tuple2<Integer, Integer>> list =
                partitionedTries.mapPartitions(new FlatMapFunction<Iterator<Tuple2<Integer, Trie>>, Tuple2<Integer, Integer>>() {
                    @Override
                    public Iterator<Tuple2<Integer, Integer>> call(Iterator<Tuple2<Integer, Trie>> tuple2Iterator) throws Exception {
                        List<Tuple2<Integer, Integer>> list = new ArrayList<>();
                        Tuple2<Integer, Trie> tuple = null;
                        for (Iterator<Tuple2<Integer, Trie>> it = tuple2Iterator; it.hasNext(); ) {
                            tuple = it.next();
                            list.add(new Tuple2<>(tuple._1() % Parallelism.PARALLELISM, 1));

                        }
                        return list.iterator();
                    }
                }, true).mapToPair(new PairFunction<Tuple2<Integer, Integer>, Integer, Integer>() {
                    @Override
                    public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> integerIntegerTuple2) throws Exception {
                        return integerIntegerTuple2;
                    }
                })
                        .reduceByKey(new Function2<Integer, Integer, Integer>() {
                            @Override
                            public Integer call(Integer v1, Integer v2) throws Exception {
                                return v1 + v2;
                            }
                        }).collect();

        System.out.println("nofTriesInPartitions::" + list);
    }

    public static void nofQueriesOnEachNode(JavaPairRDD<Integer, Query> queries, PartitioningMethods method) {

        List<Tuple2<Integer, Integer>> list = queries.mapToPair(new PairFunction<Tuple2<Integer, Query>, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Query> integerQueryTuple2) throws Exception {
                int number = getQueryPartitioningID(method, integerQueryTuple2);
                return new Tuple2<>(number % Parallelism.PARALLELISM, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }).collect();

        System.out.println("nofQueriesOnEachNode::" + list);
    }

    private static int getPartitioningID(Tuple2<Integer, Trajectory> integerQueryTuple2) {
     return integerQueryTuple2._2().getPartitionID();
    }

    private static int getQueryPartitioningID(PartitioningMethods method, Tuple2<Integer, Query> integerQueryTuple2) {
       return integerQueryTuple2._2().getPartitionID();
    }

    public static List<Tuple2<Integer, Integer>> nofTrajsOnEachNode(JavaPairRDD<Integer, Trajectory> trajectoryDataset, JavaSparkContext sc) {

        LongAccumulator nofTrajsNode = sc.sc().longAccumulator("nofTrajsNode");
        JavaPairRDD<Integer, Integer> trajsNodes=trajectoryDataset.mapToPair(new PairFunction<Tuple2<Integer, Trajectory>, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Trajectory> integerQueryTuple2) throws Exception {
                int number = getPartitioningID( integerQueryTuple2);
                return new Tuple2<>(number % Parallelism.PARALLELISM, 1);
            }


        }).reduceByKey( (v1, v2) -> v1 + v2);
        List<Tuple2<Integer, Integer>> list = trajsNodes.map(new Function<Tuple2<Integer,Integer>, Tuple2<Integer,Integer>>() {
            @Override
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> v1) throws Exception {
                nofTrajsNode.add(v1._2 ());
                return v1;
            }
        }).collect();
        System.out.println("nofTrajectoriesOnEachNode::" + list);

        return list;


    }

    public static void nofQueriesInSlice(JavaPairRDD<Integer, Query> queries, PartitioningMethods method) {

        Map<Integer, Integer> map = queries.mapToPair(new PairFunction<Tuple2<Integer, Query>, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Query> integerQueryTuple2) throws Exception {
                int number = getQueryPartitioningID(method, integerQueryTuple2);
                return new Tuple2<>(number, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }).collectAsMap();
        TreeMap<Integer, Integer> sortedMap = new TreeMap<>(map);
        System.out.println("nofQueriesInSlice::");
        for (Map.Entry<Integer, Integer> pair : sortedMap.entrySet()) {
            System.out.println( pair.getValue());

        }

    }

    public static void nofTrajsInEachSlice(JavaPairRDD<Integer, Trajectory> trajectoryDataset, PartitioningMethods method) {

        Map<Integer, Integer> map = trajectoryDataset.mapToPair(new PairFunction<Tuple2<Integer, Trajectory>, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Trajectory> integerQueryTuple2) throws Exception {
                int number = getPartitioningID( integerQueryTuple2);
                return new Tuple2<>(number, 1);
            }
        }).reduceByKey((Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2).collectAsMap();
        TreeMap<Integer, Integer> sortedMap = new TreeMap<>(map);
        System.out.println("nofTrajsInEachSlice::"+sortedMap.keySet().stream().max(Integer::compare).get());
        for (Map.Entry<Integer, Integer> pair : sortedMap.entrySet()) {
            System.out.println(pair.getValue());

        }
    }

//    public static void nofQueriesInEachVerticalPartition(JavaPairRDD<Integer, Query> queries) {
//
//        List<Tuple2<Integer, Integer>> list=queries.mapToPair(new PairFunction<Tuple2<Integer, Query>, Integer, Integer>() {
//            @Override
//            public Tuple2<Integer, Integer> call(Tuple2<Integer, Query> integerQueryTuple2) throws Exception {
//                return new Tuple2<>(integerQueryTuple2._2().getPartitionID()%Parallelism.PARALLELISM,1);
//            }
//        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
//            @Override
//            public Integer call(Integer v1, Integer v2) throws Exception {
//                return v1+v2;
//            }
//        }).collect();
//
//        System.out.println("nofQueriesOnEachNode::"+list);
//    }

//    public static void nofTriesInPartitions(JavaPairRDD<Long, Trie> partitionedTries) {
//        partitionedTries.mapPartitions(new FlatMapFunction<Iterator<Tuple2<Long,Trie>>, Tuple2<Integer, Integer>>() {
//        })
//    }
}
