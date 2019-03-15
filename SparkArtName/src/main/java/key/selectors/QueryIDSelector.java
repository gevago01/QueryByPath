package key.selectors;

import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.Set;

/**
 * Created by giannis on 12/01/19.
 */
public class QueryIDSelector implements Function<Tuple2<Integer, Set<Integer>>, Integer> {
    @Override
    public Integer call(Tuple2<Integer, Set<Integer>> v1) throws Exception {
        return v1._1();
    }
}
