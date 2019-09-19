package utilities;

import comparators.IntegerComparator;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import map.functions.CSVRecordToTrajectory;
import org.apache.spark.api.java.JavaPairRDD;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * Created by giannis on 07/03/19.
 */
public class TrajectorySynthesizer {


    private final static int DUPLICATION_FACTOR = 1;
    public final static int PROBABILITY = 20;
    private  int minRS;
    private  int maxRS;
    private long minTimestamp;
    private long maxTimestamp;
    private long maxTrajectoryID;
    private List<Trajectory> trajectories;

    public TrajectorySynthesizer(JavaPairRDD<Integer, Iterable<CSVRecord>> recordsCached, Long minTimestamp, Long maxTimestamp, Integer trajectoryID, int minRS, Integer maxRS) {

        trajectories = recordsCached.mapValues(new CSVRecordToTrajectory()).values().collect();

        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;


        this.minRS=minRS;
        this.maxRS=maxRS;


        this.maxTrajectoryID = maxTrajectoryID;

    }

    public void synthesize() throws IOException {
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

    public void reWrite() throws IOException {
        BufferedWriter bf = new BufferedWriter(new FileWriter(new File("synthDataset.csv")));

        for (Trajectory t : trajectories) {
            writeTrajectory(t, bf);
        }

        bf.close();
    }

    public static boolean getRandomBoolean(float probability) {

        double randomValue = Math.random() * 100;//0.0 to 99.9

        return randomValue <= probability;
    }

    public void lengthSkewedDataset() throws IOException {

        BufferedWriter bf = new BufferedWriter(new FileWriter(new File(PROBABILITY + "pcLS.csv")));

        int nofTrajs = trajectories.size();

        System.out.println("nofTrajs:"+nofTrajs);

        for (int i = 0; i < nofTrajs ; i++) {
             long randomTimestamp = ThreadLocalRandom.current().nextLong(minTimestamp, maxTimestamp);


            if (getRandomBoolean(PROBABILITY)) {
//                long randomSkewedLength = ThreadLocalRandom.current().nextInt(59, 60);
                long randomSkewedLength = 60;//ThreadLocalRandom.current().nextInt(59, 60);

                for (int j = 0; j < randomSkewedLength ; j++) {
                    int randomRs = ThreadLocalRandom.current().nextInt(minRS, maxRS);
                    bf.write(i + ", " + randomTimestamp + ", " + randomRs + "\n");
                    randomTimestamp = randomTimestamp + 5;
                }

            }
            else{
                long randomLength = ThreadLocalRandom.current().nextInt(2, 59);

                for (int j = 0; j < randomLength ; j++) {
                    int randomRs = ThreadLocalRandom.current().nextInt(minRS, maxRS);
                    bf.write(i + ", " + randomTimestamp + ", " + randomRs + "\n");
                    randomTimestamp = randomTimestamp + 5;
                }

            }
        }

        bf.close();
    }

    public void lenSkewedDataset() throws IOException {

        BufferedWriter bf = new BufferedWriter(new FileWriter(new File(PROBABILITY + "pcLS.csv")));


        for (Trajectory t : trajectories) {


            long randomTimestamp = ThreadLocalRandom.current().nextLong(minTimestamp, maxTimestamp);

            if (getRandomBoolean(PROBABILITY)) {

                long randomSkewedLength = 60;//ThreadLocalRandom.current().nextInt(59, 60);


                for (int j = 0; j < randomSkewedLength ; j++) {
                    if (j<t.getRoadSegments().size()){
                        bf.write(t.getTrajectoryID() + ", " + randomTimestamp + ", " + t.getRoadSegments().getInt(j) + "\n");
                    }
                    else{
                        bf.write(t.getTrajectoryID()+ ", " + randomTimestamp + ", " + t.getRoadSegments().getInt(t.getRoadSegments().size()-1) + "\n");
                    }

                    randomTimestamp = randomTimestamp + 5;
                }

            } else {
                writeTrajectory(t, bf);
            }
        }

        bf.close();
    }

    public void timeSkewedDataset() throws IOException {

        BufferedWriter bf = new BufferedWriter(new FileWriter(new File(PROBABILITY + "pcTS.csv")));

        final long fixedRandomTimestamp = ThreadLocalRandom.current().nextLong(minTimestamp, maxTimestamp);

        for (Trajectory t : trajectories) {

            ArrayList<Long> timeDiffs = getTimeDiffs(t.getTimestamps());

            if (getRandomBoolean(PROBABILITY)) {

                long randomStartTime = fixedRandomTimestamp;
                for (int j = 0; j < t.getRoadSegments().size() ; j++) {
                    bf.write(t.getTrajectoryID() + ", " + randomStartTime + ", " + t.getRoadSegments().getInt(j) + "\n");
                    randomStartTime = randomStartTime + timeDiffs.get(j);
                }
            } else {
                writeTrajectory(t, bf);
            }
        }

        bf.close();
    }

    public void roadSegmentSkewedDataset() throws IOException {

        BufferedWriter bf = new BufferedWriter(new FileWriter(new File(PROBABILITY + "pcRS.csv")));

        Random random = new Random();
        int randomStartingRS = trajectories.get(random.nextInt(trajectories.size() - 1)).getStartingRS();

        List<Trajectory > trajList = trajectories.stream().filter(t -> t.getStartingRS() == randomStartingRS).collect(toList());
        Trajectory randomTrajectory =trajList.get(random.nextInt(trajList.size() - 1));;
        for (Trajectory t : trajectories) {


            if (getRandomBoolean(PROBABILITY)) {

                ArrayList<Long> timeDiffs = getTimeDiffs(t.getTimestamps());
                long randomStartTime = ThreadLocalRandom.current().nextLong(minTimestamp, maxTimestamp);

                int randomRs=randomStartingRS;
                for (int j = 0; j < t.getRoadSegments().size() -1; j++) {

                    bf.write(t.getTrajectoryID() + ", " + randomStartTime + ", " + randomRs + "\n");
                    randomRs = ThreadLocalRandom.current().nextInt(minRS, maxRS);
                    randomStartTime = randomStartTime + timeDiffs.get(j);


                }
            } else {
                writeTrajectory(t, bf);
            }
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
