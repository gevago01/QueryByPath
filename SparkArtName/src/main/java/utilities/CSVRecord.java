package utilities;

import java.io.Serializable;

/**
 * Created by giannis on 10/12/18.
 */
public class CSVRecord implements Serializable {

    int trajID;
    long timestamp;
    int roadSegment;

    public int getTrajID() {
        return trajID;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public int getRoadSegment() {
        return roadSegment;
    }

    @Override
    public String toString() {
        return "CSVRecord{" +
                "trajID=" + trajID +
                ", timestamp=" + timestamp +
                ", roadSegment='" + roadSegment + '\'' +
                '}';
    }


    public CSVRecord(String trajectoryID, String timestampStr, String roadSegmentId) {

        trajID=Integer.parseInt(trajectoryID.trim());
        timestamp=Long.parseLong(timestampStr.trim());
        roadSegment=Integer.parseInt(roadSegmentId.trim());

    }
}
