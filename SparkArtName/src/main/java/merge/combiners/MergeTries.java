package merge.combiners;

import it.unimi.dsi.fastutil.longs.Long2LongAVLTreeMap;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.objects.ObjectSortedSet;
import org.apache.spark.api.java.function.Function2;
import trie.Node;
import trie.Trie;
import utilities.Trajectory;

import java.util.Collection;
import java.util.Stack;
import java.util.TreeMap;

/**
 * Created by giannis on 03/01/19.
 */
public class MergeTries implements Function2<Trie, Trie, Trie> {

    @Override
    public Trie call(Trie trie1, Trie trie2) throws Exception {

//        System.out.println("COmbining trie:"+trie1.getStartingRS()+" with:"+trie2.getStartingRS());
//
//        trie2.appendTrajContainer(trie1.getTrajectoryContainer());
        return trie2;
    }
}
