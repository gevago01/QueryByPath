package key.selectors;

import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import trie.Trie;
import utilities.CSVRecord;
import utilities.Trajectory;

/**
 * Created by giannis on 11/12/18.
 */
public class TrajectoryTSSelector implements Function<Trajectory, Integer> {
    @Override
    public Integer call(Trajectory trajectory) throws Exception {
        return trajectory.timeSlice;
    }
}
