package utilities;

import comparators.IntegerComparator;
import comparators.LongComparator;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import key.selectors.CSVTrajIDSelector;
import map.functions.CSVRecordToTrajectory;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import projections.ProjectRoadSegments;
import projections.ProjectTimestamps;

import java.io.*;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by giannis on 07/03/19.
 */
public class TrajectorySynthesizerME implements Serializable {


    private final static int DUPLICATION_FACTOR = 1;
    private long minTimestamp;
    private long maxTimestamp;
    private long maxTrajectoryID;
    private JavaRDD<Trajectory> trajectories;

    public TrajectorySynthesizerME(JavaRDD<CSVRecord> records) {
        JavaPairRDD<Integer, Iterable<CSVRecord>> recordsCached = records.groupBy(new CSVTrajIDSelector()).cache();


        trajectories = recordsCached.mapValues(new CSVRecordToTrajectory()).values().cache();
        JavaRDD<Long> timestamps = records.map(new ProjectTimestamps());
        minTimestamp = timestamps.min(new LongComparator());
        maxTimestamp = timestamps.max(new LongComparator());
        maxTrajectoryID = records.map(new ProjectRoadSegments()).max(new IntegerComparator());

    }

    public void synthesize() throws IOException {


        trajectories.coalesce(1).foreach(new VoidFunction<Trajectory>() {
            BufferedWriter bf = new BufferedWriter(new FileWriter(new File("synthDataset.csv")));

            @Override
            public void call(Trajectory t) throws Exception {
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
        });

    }

    public void timeSkewedDataset() throws IOException {



//        trajectories.values().foreach(t -> t.setTimestampAt(0,1));

        trajectories.coalesce(1).foreach(new VoidFunction<Trajectory>() {
            int counter = 0;
            final long randomTimestamp = ThreadLocalRandom.current().nextLong(minTimestamp, maxTimestamp);

            @Override
            public void call(Trajectory t) throws Exception {
                ArrayList<Long> timeDiffs = getTimeDiffs(t.getTimestamps());

                if (counter % 10 == 0) {

                    int j;
                    long randomStartTime = randomTimestamp;
                    for (j = 0; j < t.getRoadSegments().size()-1 ; j++) {
                        t.setTimestampAt(j, randomStartTime);
                         randomStartTime+= timeDiffs.get(j);
                    }
                t.setTimestampAt(j, randomStartTime);
                }
                ++counter;

            }
        });


        trajectories.saveAsTextFile("timeSkewedDataset");

    }

    public  void roadSegmentSkewedDataset( ) throws IOException {



        Random random = new Random();

        //TODO choose sample traj
                Trajectory randomTrajectory = trajectories.takeSample(false,1).get(0);

        trajectories.coalesce(1).foreach(new VoidFunction<Trajectory>() {
            int counter = 0;

            @Override
            public void call(Trajectory t) throws Exception {
                if (counter % 10 == 0) {
                    for (int j = 0; j < randomTrajectory.getRoadSegments().size(); j++) {
                        t.setTimestampAt(j, randomTrajectory.getTimestamps().getLong(j));
                        t.setRoadSegmentAt(j, randomTrajectory.getRoadSegments().getInt(j));
                    }
                }
                ++counter;

            }
        });

        trajectories.saveAsTextFile("roadSegmentSkewedDataset");

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
