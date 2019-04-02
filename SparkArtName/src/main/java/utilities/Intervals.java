package utilities;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by giannis on 29/03/19.
 */
public class Intervals {

    public static List<Long> getIntervals(int numberOfSlices, long minValue, long maxValue) {
        final Long timeInt = (maxValue - minValue) / numberOfSlices;

        List<Long> intervals = new ArrayList<>();

        long value=minValue;
        for (int i = 0; i < numberOfSlices - 1; i++) {
            intervals.add(value);
            value+=timeInt;
        }
        intervals.add(maxValue);


        return intervals;
    }
}
