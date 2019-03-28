package utilities;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import partitioning.methods.VerticalPartitioning;

/**
 * Created by giannis on 28/03/19.
 */
public class GroupSizes {


    public static void mesaureGroupSize(JavaPairRDD<Integer, Trajectory> trajectoryDataset, String fileName) {
        JavaPairRDD<Integer,Integer> groupSizes=      trajectoryDataset.groupByKey().mapValues(new Function<Iterable<Trajectory>, Integer>() {
            @Override
            public Integer call(Iterable<Trajectory> v1) throws Exception {

                int i=0;
                for (Trajectory t:v1) {
                    ++i;
                }
                return i;
            }
        });

        groupSizes.count();
//        groupSizes.saveAsTextFile(fileName +".txt");

        System.exit(1);
    }
}
