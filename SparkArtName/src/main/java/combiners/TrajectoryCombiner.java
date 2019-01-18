package combiners;

import org.apache.spark.api.java.function.Function;
import trie.Trie;
import utilities.Trajectory;

/**
 * Created by giannis on 03/01/19.
 */
public class TrajectoryCombiner implements Function<Trajectory, Trie> {
    @Override
    public Trie call(Trajectory trajectory) throws Exception {

        Trie trie=new Trie();
        trie.setStartingRoadSegment(trajectory.getStartingRS());
        trie.insertTrajectory2(trajectory);
        return trie;
    }
}
