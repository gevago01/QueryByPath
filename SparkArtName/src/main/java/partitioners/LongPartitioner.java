package partitioners;

import org.apache.spark.Partitioner;
import partitioning.methods.TimeSlicing;
import utilities.Parallelism;

/**
 * Created by giannis on 11/12/18.
 * This class is being used for Vertical Partitioning
 * for partitioning the road segment ids
 */
public class LongPartitioner extends Partitioner {
    private int numOfPartitions;

    public LongPartitioner(int numOfPartitions){
        this.numOfPartitions=numOfPartitions;
    }
    @Override
    public int numPartitions() {
        return numOfPartitions;
    }

    @Override
    public int getPartition(Object trieObject) {

        Long startingRS  = (Long) trieObject;
        int hashValue = Long.hashCode(startingRS);
        return Math.abs(hashValue) % numPartitions();
    }
}
