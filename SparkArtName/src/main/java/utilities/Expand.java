package utilities;

import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Created by giannis on 19/01/20.
 */
public class Expand {


    public static void expandDataset(JavaRDD<CSVRecord> records) {

        List<Tuple2<Integer, Iterable<CSVRecord>>> trajRecords = records. groupBy(csv -> csv.getTrajID()).collect();


        try {
            BufferedWriter bw=new BufferedWriter(new FileWriter("expanded"));

            for (Tuple2<Integer, Iterable<CSVRecord>> tuple:trajRecords) {

                List<Iterable<CSVRecord>> allSubTrajs = expandTrajectory(tuple._2());

                allSubTrajs.add(tuple._2());

                writeToFile(allSubTrajs,bw);

            }

            bw.flush();
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void writeToFile(List<Iterable<CSVRecord>> allSubTrajs, BufferedWriter bw) throws IOException {

        for (Iterable<CSVRecord> subT:allSubTrajs) {

            for (CSVRecord trajPoint:subT) {

                bw.write(trajPoint.getTrajID()+","+trajPoint.getTimestamp()+","+trajPoint.getRoadSegment()+"\n");

            }

        }
    }

    private static List<Iterable<CSVRecord>> expandTrajectory(Iterable<CSVRecord> csvRecords) {

        ArrayList<CSVRecord> csvRecordList = Lists.newArrayList(csvRecords);
        csvRecordList.sort(Comparator.comparing(CSVRecord::getTimestamp));

        List<Iterable<CSVRecord>> allRecordCombinations = new ArrayList<>();
        for (int i = 2; i < csvRecordList.size() + 1; i++) {
            List<CSVRecord> subL = csvRecordList.subList(0, i);
            allRecordCombinations.add(subL);

        }

        for (int i = csvRecordList.size() - 2; i > 0; i--) {
            List<CSVRecord> subL = csvRecordList.subList(i, csvRecordList.size());
            allRecordCombinations.add(subL);

        }

        return allRecordCombinations;
    }
}
