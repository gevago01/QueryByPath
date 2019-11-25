package comparators;

import scala.Serializable;
import scala.Tuple2;

/**
 * Created by giannis on 26/09/19.
 */
public class TupleIntIntComparator implements java.util.Comparator<scala.Tuple2<Integer, Integer>> , java.io.Serializable {

    @Override
    public int compare(Tuple2<Integer, Integer> t2, Tuple2<Integer, Integer> t1) {


        return Integer.compare(t2._2(),t1._2());
    }
}
