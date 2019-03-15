package comparators;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Created by giannis on 15/01/19.
 */
public class IntegerComparator implements Comparator<Integer>, Serializable {
    @Override
    public int compare(Integer a, Integer b) {
        return Integer.compare(a, b);
    }
}
