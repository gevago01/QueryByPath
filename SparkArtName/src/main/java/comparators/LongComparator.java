package comparators;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Created by giannis on 15/01/19.
 */
public class LongComparator implements Comparator<Long>,Serializable {
    @Override
    public int compare(Long a, Long b) {
        if (a==b){
            return 0;
        }
        else if(a<b){
            return -1;
        }
        else {
            return 1;
        }
    }
}
