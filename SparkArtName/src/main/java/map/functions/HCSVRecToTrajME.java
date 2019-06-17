package map.functions;

import com.google.common.collect.Lists;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.apache.spark.api.java.function.Function;
import utilities.CSVRecord;
import utilities.Trajectory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Created by giannis on 11/12/18.
 */
public class HCSVRecToTrajME implements Function<Iterable<CSVRecord>, Iterable<Trajectory>> {

    private int HORIZONTAL_PARTITION_SIZE;

    public HCSVRecToTrajME(int partitionSize) {
        HORIZONTAL_PARTITION_SIZE = partitionSize;
    }

    public static int max=Integer.MIN_VALUE;

//    public HCSVRecToTrajME(List<Long> intervals) {
//        List<Trajectory> trajectoryList = new ArrayList<>();
//        ArrayList<CSVRecord> csvRecordList = Lists.newArrayList(csvRecords);
//
//    }

    @Override
    public Iterable<Trajectory> call(Iterable<CSVRecord> csvRecords) throws Exception {

        List<Trajectory> trajectoryList = new ArrayList<>();
        ArrayList<CSVRecord> csvRecordList = Lists.newArrayList(csvRecords);
        LongArrayList startingEnding=new LongArrayList();

        if (csvRecordList.size()>max){
            max=csvRecordList.size();
        }
        csvRecordList.sort(Comparator.comparing(CSVRecord::getTimestamp));
        List<List<CSVRecord>> allSubTrajs = Lists.partition(csvRecordList, HORIZONTAL_PARTITION_SIZE);


//        if (allSubTrajs.size()>10) {
//            System.out.println("allSubTrajs.size():" + allSubTrajs.size());
//
//        }
        int subTrajID = 0;
        for (List<CSVRecord> subTrajectory : allSubTrajs) {
            Trajectory mo = null;
            CSVRecord previous = null;
            for (CSVRecord csvRec : subTrajectory) {
                if (mo == null) {
                    mo = new Trajectory(csvRec.getTrajID());
                    startingEnding.add(csvRec.getTimestamp());
                    mo.setPartitionID(subTrajID++);
                }
                mo.addRoadSegment(csvRec.getRoadSegment());
                previous = csvRec;
            }
            startingEnding.add(previous.getTimestamp());

            trajectoryList.add(mo);
        }


        return trajectoryList;
    }
}
