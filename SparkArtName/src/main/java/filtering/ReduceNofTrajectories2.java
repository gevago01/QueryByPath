package filtering;

import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import utilities.Trajectory;

/**
 * Created by giannis on 14/01/19.
 */
public class ReduceNofTrajectories2 implements Function<Tuple2<Integer, Trajectory>, Boolean> {

    public final static int MAX_TRAJECTORY_SIZE=100;
    @Override
    public Boolean call(Tuple2<Integer, Trajectory> v1) throws Exception {
        return v1._2().getRoadSegments().size() < MAX_TRAJECTORY_SIZE ? true : false;
    }
}
