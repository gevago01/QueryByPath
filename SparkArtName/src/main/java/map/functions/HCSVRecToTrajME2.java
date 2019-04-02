package map.functions;

import com.google.common.collect.Lists;
import org.apache.spark.api.java.function.Function;
import utilities.CSVRecord;
import utilities.Trajectory;

import java.util.*;

/**
 * Created by giannis on 11/12/18.
 */
public class HCSVRecToTrajME2 implements Function<Iterable<CSVRecord>, Iterable<Trajectory>> {

    private final List<Long> intervals;

    public HCSVRecToTrajME2(List<Long> intervals) {
        this.intervals = intervals;

    }

    @Override
    public Iterable<Trajectory> call(Iterable<CSVRecord> csvRecords) throws Exception {
//        TreeMap<Integer, Integer> indexes = new TreeMap<>();
        List<Trajectory> trajectoryList = new ArrayList<>();
        ArrayList<CSVRecord> csvRecordList = Lists.newArrayList(csvRecords);

        csvRecordList.sort(Comparator.comparing(CSVRecord::getTimestamp));


        int trajectoryLength = csvRecordList.size();

        TreeMap<Integer, Integer> indexes = HCSVRecToTrajME2.getIndexPositions(trajectoryLength, intervals);
        List<List<CSVRecord>> allSubTrajs = new ArrayList<>();

        for (Map.Entry<Integer, Integer> indexPair : indexes.entrySet()) {
            int rightIndex = intervals.get(indexPair.getValue()).intValue();
            int leftIndex = intervals.get(indexPair.getKey()).intValue();
            allSubTrajs.add(csvRecordList.subList(leftIndex, rightIndex >= trajectoryLength ? trajectoryLength : rightIndex));
        }


        int subTrajID = 0;
        for (List<CSVRecord> subTrajectory : allSubTrajs) {
            Trajectory mo = null;
            CSVRecord previous = null;
            for (CSVRecord csvRec : subTrajectory) {
                if (mo == null) {
                    mo = new Trajectory(csvRec.getTrajID());
                    mo.setStartingTime(csvRec.getTimestamp());
                    mo.setHorizontalID(subTrajID++);
                }
                mo.addRoadSegment(csvRec.getRoadSegment());
                previous = csvRec;
            }
            try {
                mo.setEndingTime(previous.getTimestamp());
            } catch (NullPointerException e) {
                System.out.println();
            }
            trajectoryList.add(mo);
        }


        return trajectoryList;
    }

    public static TreeMap<Integer, Integer> getIndexPositions(int trajectoryLength, List<Long> intervals) {

        TreeMap<Integer, Integer> indexes = new TreeMap<>();
        //first loop might be skipped if trajectory is too short
        indexes.put(0, 1);
        int until;
        for (until = 0; until < intervals.size()-1; until++) {
            if (trajectoryLength >= intervals.get(until) && trajectoryLength <= intervals.get(until + 1)) {

                break;
            }
        }


        for (int i = 0; i <= until; i++) {
            indexes.put(i, i + 1);
        }


        return indexes;
    }
}
