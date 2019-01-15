package projections;

import org.apache.spark.api.java.function.Function;
import utilities.CSVRecord;

/**
 * Created by giannis on 15/01/19.
 */
public class ProjectTimestamps implements Function<CSVRecord, Long> {
    public Long call(CSVRecord record) throws Exception {
        return record.getTimestamp();
    }
}
