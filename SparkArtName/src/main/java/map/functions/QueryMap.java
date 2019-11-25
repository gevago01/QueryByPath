package map.functions;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.DoubleAccumulator;
import scala.Tuple2;
import trie.Query;
import trie.Trie;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Created by giannis on 19/08/19.
 */
public class QueryMap  implements Function<Tuple2<Integer, Trie>, Set<Integer>> {
    private final Broadcast<List<Tuple2<Integer, Query>>> partBroadQueries;
    private final DoubleAccumulator joinTimeAcc;

    public QueryMap(Broadcast<List<Tuple2<Integer, Query>>> partBroadQueries, DoubleAccumulator joinTimeAcc) {
        this.partBroadQueries=partBroadQueries;
        this.joinTimeAcc=joinTimeAcc;
    }

    @Override
    public Set<Integer> call(Tuple2<Integer, Trie> v1) throws Exception {
        List<Tuple2<Integer, Query>> localQueries = partBroadQueries.value();
        Set<Integer> answer = new TreeSet<>();

        for (Tuple2<Integer, Query> queryEntry : localQueries) {

            Set<Integer> ans = null;
            long t1 = System.nanoTime();
//            if (queryEntry._2().getStartingRoadSegment() >= v1._2().getMinStartingRS() && queryEntry._2().getStartingRoadSegment() <= v1._2().getMaxStartingRS()) {
                if (queryEntry._2().getStartingTime() >= v1._2().getMinStartingTime() && queryEntry._2().getStartingTime() <= v1._2().getMaxStartingTime()) {
                    ans = v1._2().queryIndex(queryEntry._2());
                }
//            }
            if (ans!=null) {
                answer.addAll(ans);
            }

            long t2 = System.nanoTime();
            long diff = t2 - t1;
            joinTimeAcc.add(diff * Math.pow(10.0, -9.0));
        }
        return answer;
    }
}
