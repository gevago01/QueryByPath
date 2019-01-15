package projections;

import org.apache.spark.api.java.function.Function;
import utilities.CSVRecord;

/**
 * Created by giannis on 15/01/19.
 */
public class ProjectRoadSegments implements Function<CSVRecord, Long> {
    @Override
    public Long call(CSVRecord v1) throws Exception {
        return v1.getRoadSegment();
    }
}
