package utilities;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import trie.Query;
import trie.Trie;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
        List<Tuple2<Integer, Integer>> list=
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
                }, false).groupBy(new Function<Tuple2<Integer, Integer>, Integer>() {
                    @Override
                    public Integer call(Tuple2<Integer, Integer> v1) throws Exception {
                        return v1._1();
                    }
                }).flatMapValues(new Function<Iterable<Tuple2<Integer, Integer>>, Iterable<Tuple2<Integer, Integer>>>() {
                    @Override
                    public Iterable<Tuple2<Integer, Integer>> call(Iterable<Tuple2<Integer, Integer>> v1) throws Exception {

                        List<Tuple2<Integer, Integer>> list = new ArrayList<>();
                        Tuple2<Integer, Integer> lastTuple = null;
                        int sum = 0;
                        for (Tuple2<Integer, Integer> t : v1) {
                            sum += t._2();
                            lastTuple = t;
                        }
                        list.add(new Tuple2<>(lastTuple._1(), sum));
                        return list;
                    }
                }).values().collect();
        System.out.println("nofTriesInPartitions::"+list);
    }
}
