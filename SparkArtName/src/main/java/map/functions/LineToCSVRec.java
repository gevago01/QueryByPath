package map.functions;

import org.apache.spark.api.java.function.Function;
import utilities.CSVRecord;

/**
 * Created by giannis on 11/01/19.
 */
public class LineToCSVRec implements Function<String, CSVRecord> {
    @Override
    public CSVRecord call(String s) throws Exception {
        String[] tokens = s.split(",");

        return new CSVRecord(tokens[0], tokens[1], tokens[2]);
    }
}
