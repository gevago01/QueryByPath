package map.functions;

import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import trie.Trie;
import utilities.Trajectory;

import java.util.NoSuchElementException;

/**
 * Created by giannis on 19/09/19.
 */
public class AddTrajToTrie implements Function<Tuple2<Iterable<Trie>, Iterable<Trajectory>>, Trie> {
    @Override
    public Trie call(Tuple2<Iterable<Trie>, Iterable<Trajectory>> v1) throws Exception {
        Trie trie = v1._1().iterator().next();

        for (Trajectory trajectory : v1._2()) {
            trie.insertTrajectory2(trajectory);
        }
        return trie;
    }
}
