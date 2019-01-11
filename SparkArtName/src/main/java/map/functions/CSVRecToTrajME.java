package map.functions;

import com.google.common.collect.Lists;
import org.apache.spark.api.java.function.Function;
import utilities.CSVRecord;
import utilities.Trajectory;

import java.util.ArrayList;
import java.util.Comparator;

/**
 * Created by giannis on 11/12/18.
 * For memory efficiency we only use the starting and ending
 * timestamp and omit all intermediate timestamps
 */
public class CSVRecToTrajME implements Function<Iterable<CSVRecord>, Trajectory> {



    @Override
    public Trajectory call(Iterable<CSVRecord> csvRecords) throws Exception {

        ArrayList<CSVRecord> csvRecordList = Lists.newArrayList(csvRecords);

        csvRecordList.sort(Comparator.comparing(CSVRecord::getTimestamp));
        Trajectory mo = null;

        CSVRecord previous=null;
        for (CSVRecord csvRec:csvRecordList) {
            if (mo == null) {
                mo = new Trajectory(csvRec.getTrajID());
                mo.setStartingTime(csvRec.getTimestamp());
            }
            mo.addRoadSegment(csvRec.getRoadSegment());
            previous=csvRec;
        }
        mo.setEndingTime(previous.getTimestamp());
        return mo;
    }
}
