package map.functions;

import com.google.common.collect.Lists;
import org.apache.spark.api.java.function.Function;
import utilities.CSVRecord;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Created by giannis on 11/12/19.
 */
public class CSVRecToAllSubTrajs2 implements Function<Iterable<CSVRecord>, Iterable<Iterable<CSVRecord>>> {
    @Override
    public Iterable<Iterable<CSVRecord>> call(Iterable<CSVRecord> csvRecords) throws Exception {
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
