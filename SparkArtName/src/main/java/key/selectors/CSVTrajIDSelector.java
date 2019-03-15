package key.selectors;

import org.apache.spark.api.java.function.Function;
import utilities.CSVRecord;

/**
 * Created by giannis on 11/12/18.
 */
public class CSVTrajIDSelector implements Function<CSVRecord, Integer> {
    @Override
    public Integer call(CSVRecord csvRecord) throws Exception {
        return csvRecord.getTrajID();
    }
}
