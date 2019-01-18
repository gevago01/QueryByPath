package merge.values;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import trie.Trie;
import utilities.Trajectory;

/**
 * Created by giannis on 03/01/19.
 */
public class MergeTrajectory implements Function2< Trie,Trajectory,Trie> {
    @Override
    public Trie call(Trie trie, Trajectory trajectory) throws Exception {
        System.out.println("Adding traj with SRS:"+trajectory.getStartingRS()+" to trie with SRS:"+trie.getStartingRS());
        trie.insertTrajectory2(trajectory);
        return trie;
    }
}
