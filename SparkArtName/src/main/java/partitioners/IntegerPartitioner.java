package partitioners;

import org.apache.spark.Partitioner;
import partitioning.methods.TimeSlicing;
import trie.Trie;
import utilities.Parallelism;

/**
 * Created by giannis on 11/12/18.
 * This class is being used by Horizontal Partitioning
 * and the Time Slicing methods
 */
public class IntegerPartitioner extends Partitioner {

    private int numOfPartitions;
    public IntegerPartitioner(int numOfPartitions ) {
        this.numOfPartitions=numOfPartitions;
    }

    @Override
    public int numPartitions() {
        return numOfPartitions;
//        return Parallelism.PARALLELISM;
    }

    @Override
    public int getPartition(Object trieObject) {
        //this integer can be either a
        //horizontalPartitionID or a timeSliceID
        Integer integer  = (Integer) trieObject;
        int hashValue = Integer.hashCode(integer);
        return Math.abs(hashValue) % numPartitions();
    }
}
