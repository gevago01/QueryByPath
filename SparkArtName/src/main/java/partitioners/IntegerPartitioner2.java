package partitioners;

import org.apache.spark.Partitioner;
import utilities.Parallelism;

/**
 * Created by giannis on 11/12/18.
 * This class is being used by Horizontal Partitioning
 * and the Time Slicing methods
 */
public class IntegerPartitioner2 extends Partitioner {

    public IntegerPartitioner2() {
    }

    @Override
    public int numPartitions() {
//        return 3;
        return 221978;
    }

    @Override
    public int getPartition(Object trieObject) {
        //this integer can be either a
        //horizontalPartitionID or a timeSliceID
        Integer integer  = (Integer) trieObject;
//        int hashValue = Integer.hashCode(integer);
//        return Math.abs(hashValue) % numPartitions();
        return integer % numPartitions();
    }
}
