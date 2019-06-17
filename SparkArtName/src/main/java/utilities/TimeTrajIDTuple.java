package utilities;

import it.unimi.dsi.fastutil.ints.IntArraySet;

/**
 * Created by giannis on 19/04/19.
 */
public class TimeTrajIDTuple implements Comparable<TimeTrajIDTuple> {
    @Override
    public String toString() {
        return "TimeTrajIDTuple{" +
                "timestamp=" + timestamp +
                ", trajIDs=" + trajIDs +
                '}';
    }

    private long timestamp;
    private IntArraySet trajIDs = new IntArraySet();


    public long getTimestamp() {
        return timestamp;
    }

    public IntArraySet getTrajIDs() {
        return trajIDs;
    }

    @Override
    public int compareTo(TimeTrajIDTuple timeTrajIDTuple) {
        return Long.compare(timestamp,timeTrajIDTuple.timestamp);
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void addTrajID(int trajectoryID) {
        trajIDs.add(trajectoryID);
    }
}
