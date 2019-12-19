package map.functions;

import com.google.common.collect.Lists;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import utilities.CSVRecord;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Created by giannis on 11/12/19.
 */
public class CSVRecToAllSubTrajs implements Function<Iterable<CSVRecord>, Iterable<Iterable<CSVRecord>>> {
    @Override
    public Iterable<Iterable<CSVRecord>> call(Iterable<CSVRecord> csvRecords) throws Exception {
        ArrayList<CSVRecord> csvRecordList = Lists.newArrayList(csvRecords);
        csvRecordList.sort(Comparator.comparing(CSVRecord::getTimestamp));

        List<Iterable<CSVRecord>> allRecordCombinations = new ArrayList<>();
        for (int i = 1; i < csvRecordList.size() ; i++) {

            List<List<CSVRecord>> allCombinationsOfSizeI= Lists.partition(csvRecordList,i);
            for (List<CSVRecord> sizeIList:allCombinationsOfSizeI) {

                allRecordCombinations.add(sizeIList);

            }
        }

        return allRecordCombinations;

    }



}
