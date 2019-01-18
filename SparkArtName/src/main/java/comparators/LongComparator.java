package comparators;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Created by giannis on 15/01/19.
 */
public class LongComparator implements Comparator<Long>, Serializable {
    @Override
    public int compare(Long a, Long b) {
        return Long.compare(a, b);
    }
}
