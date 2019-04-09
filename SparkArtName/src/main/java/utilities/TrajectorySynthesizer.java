package utilities;

import comparators.IntegerComparator;
import comparators.LongComparator;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import key.selectors.CSVTrajIDSelector;
import map.functions.CSVRecordToTrajectory;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import projections.ProjectRoadSegments;
import projections.ProjectTimestamps;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by giannis on 07/03/19.
 */
public class TrajectorySynthesizer {


    private final static int DUPLICATION_FACTOR = 1;
    private long minTimestamp;
    private long maxTimestamp;
    private long maxTrajectoryID;
    private List<Trajectory> trajectories;

    public TrajectorySynthesizer(JavaRDD<CSVRecord> records) {
        JavaPairRDD<Integer, Iterable<CSVRecord>> recordsCached = records.groupBy(new CSVTrajIDSelector()).cache();

        trajectories = recordsCached.mapValues(new CSVRecordToTrajectory()).values().collect();
        JavaRDD<Long> timestamps = records.map(new ProjectTimestamps());
        minTimestamp = timestamps.min(new LongComparator());
        maxTimestamp = timestamps.max(new LongComparator());
        maxTrajectoryID = records.map(new ProjectRoadSegments()).max(new IntegerComparator());

    }

    public  void synthesize( ) throws IOException {
        BufferedWriter bf = new BufferedWriter(new FileWriter(new File("synthDataset.csv")));

        for (Trajectory t : trajectories) {

            ArrayList<Long> timeDiffs = getTimeDiffs(t.getTimestamps());

            for (int i = 0; i < DUPLICATION_FACTOR; i++) {

                long timestamp = ThreadLocalRandom.current().nextLong(minTimestamp, maxTimestamp);
                int j;
                for (j = 0; j < t.getRoadSegments().size() - 1; j++) {
                    bf.write(maxTrajectoryID + ", " + timestamp + ", " + t.getRoadSegments().getInt(j) + "\n");
                    timestamp = timestamp + timeDiffs.get(j);
                }
                bf.write(maxTrajectoryID + ", " + timestamp + ", " + t.getRoadSegments().getInt(j) + "\n");
                ++maxTrajectoryID;
            }
            writeTrajectory(t, bf);
        }

        bf.close();
    }

    public  void timeSkewedDataset( ) throws IOException {

        int counter = 0;
        BufferedWriter bf = new BufferedWriter(new FileWriter(new File("timeSkewedDataset.csv")));

        long randomTimestamp = ThreadLocalRandom.current().nextLong(minTimestamp, maxTimestamp);

        for (Trajectory t : trajectories) {

            ArrayList<Long> timeDiffs = getTimeDiffs(t.getTimestamps());

            if (counter % 10 == 0) {

                int j;
                long randomStartTime = randomTimestamp;
                for (j = 0; j < t.getRoadSegments().size() - 1; j++) {
                    bf.write(t.getTrajectoryID() + ", " + randomStartTime + ", " + t.getRoadSegments().getInt(j) + "\n");
                    randomStartTime = randomStartTime + timeDiffs.get(j);
                }
                bf.write(t.getTrajectoryID() + ", " + randomStartTime + ", " + t.getRoadSegments().getInt(j) + "\n");
            } else {
                writeTrajectory(t, bf);
            }
            ++counter;
        }

        bf.close();
    }

    public  void roadSegmentSkewedDataset( ) throws IOException {

        int counter = 0;
        BufferedWriter bf = new BufferedWriter(new FileWriter(new File("roadSegmentSkewedDataset.csv")));

        Random random = new Random();

        Trajectory randomTrajectory = trajectories.get(random.nextInt(trajectories.size()));

        for (Trajectory t : trajectories) {


            if (counter % 10 == 0) {
                for (int j = 0; j < randomTrajectory.getRoadSegments().size(); j++) {
                    bf.write(t.getTrajectoryID() + ", " + randomTrajectory.getTimestamps().getLong(j) + ", " + randomTrajectory.getRoadSegments().getInt(j) + "\n");
                }
            } else {
                writeTrajectory(t, bf);
            }
            ++counter;
        }

        bf.close();
    }

    private static ArrayList<Long> getTimeDiffs(LongArrayList timestamps) {
        ArrayList<Long> timeDiffs = new ArrayList<>();
        for (int j = 1; j < timestamps.size(); j++) {
            long timeDiff = timestamps.getLong(j) - timestamps.getLong(j - 1);

            timeDiffs.add(timeDiff);
        }

        return timeDiffs;
    }

    private static void writeTrajectory(Trajectory t, BufferedWriter bf) throws IOException {


        for (int i = 0; i < t.getRoadSegments().size(); i++) {
            bf.write(t.getTrajectoryID() + ", " + t.getTimestamps().getLong(i) + ", " + t.getRoadSegments().getInt(i) + "\n");
        }

    }
}
