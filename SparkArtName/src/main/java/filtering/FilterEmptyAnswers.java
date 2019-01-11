package filtering;

import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.Set;

/**
 * Created by giannis on 11/01/19.
 */
public class FilterEmptyAnswers implements Function<Tuple2<Integer, Set<Long>>, Boolean> {
    @Override
    public Boolean call(Tuple2<Integer, Set<Long>> v1) throws Exception {
        return v1._2().isEmpty() ? false : true;
    }
}
