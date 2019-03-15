package utilities;

import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by giannis on 07/03/19.
 */
public class TrajectorySynthesizer {


    private final static int DUPLICATION_FACTOR = 1;

    public static void synthesize(List<Trajectory> trajectories, long minTimestamp, long maxTimestamp, long maxTrajID) throws IOException {

        long maxTrajectoryID = maxTrajID + 1;


        BufferedWriter bf = new BufferedWriter(new FileWriter(new File("synthDataset.csv")));

        for (Trajectory t : trajectories) {

            ArrayList<Long> timeDiffs = getTimeDiffs(t.getTimestamps());

            for (int i = 0; i < DUPLICATION_FACTOR; i++) {

                long timestamp = ThreadLocalRandom.current().nextLong(minTimestamp, maxTimestamp);
                int j;
                for (j = 0; j < t.getRoadSegments().size() - 1; j++) {
                    bf.write(maxTrajectoryID + ", " + timestamp + ", " + t.getRoadSegments().get(j) + "\n");
                    timestamp = timestamp + timeDiffs.get(j);
                }
                bf.write(maxTrajectoryID + ", " + timestamp + ", " + t.getRoadSegments().get(j) + "\n");

                ++maxTrajectoryID;

            }

            writeTrajectory(t, bf);

        }


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
            bf.write(t.getTrajectoryID() + ", " + t.getTimestamps().getLong(i) + ", " + t.getRoadSegments().get(i) + "\n");
        }

    }
}
