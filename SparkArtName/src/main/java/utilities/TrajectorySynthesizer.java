package utilities;

import com.google.common.collect.Iterables;
import comparators.IntegerComparator;
import comparators.LongComparator;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import map.functions.CSVRecordToTrajectory;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import javax.xml.bind.SchemaOutputResolver;
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
    public final static int DATASET_SIZE = 8000000;
    private final int avgLen;
    private final int medianLen;
    private int sumLen;
    private  int minRS;
    private  int maxRS;
    private  int minLen;
    private  int maxLen;
    private long minTimestamp;
    private long maxTimestamp;
    private long maxTrajectoryID;
    private List<Trajectory> trajectories;

    public TrajectorySynthesizer(JavaRDD<CSVRecord> records) {

        JavaPairRDD<Integer, Iterable<CSVRecord>> recordsCached = records.groupBy(csv -> csv.getTrajID()).cache();
        trajectories = recordsCached.mapValues(new CSVRecordToTrajectory()).values().collect();

        this.minTimestamp = records.map(r -> r.getTimestamp()).min(new LongComparator());
        this.maxTimestamp = records.map(r -> r.getTimestamp()).max(new LongComparator());


        this.minRS=records.map(r -> r.getRoadSegment()).min(new IntegerComparator());
        this.maxRS=records.map(r -> r.getRoadSegment()).max(new IntegerComparator());


        this.minLen = recordsCached.map(r -> Iterables.size(r._2())).min(new IntegerComparator());
        this.maxLen = recordsCached.map(r -> Iterables.size(r._2())).max(new IntegerComparator());
        this.sumLen = recordsCached.map(r -> Iterables.size(r._2())).reduce((l1,l2) -> l1+l2);

        this.avgLen = sumLen/trajectories.size();

        this.medianLen = (this.avgLen+this.minLen)/2;
        this.maxTrajectoryID = records.map(r -> r.getTrajID()).max(new IntegerComparator());

    }

    public void synthesize() throws IOException {
        BufferedWriter bf = new BufferedWriter(new FileWriter(new File("objects"+DATASET_SIZE+"_synthDataset.csv")));

        for (int i = 0; i < DATASET_SIZE; i++) {

            if (i>=trajectories.size()){

                ++maxTrajectoryID;
                long timestamp = ThreadLocalRandom.current().nextLong(minTimestamp, maxTimestamp);
                long randomSize=ThreadLocalRandom.current().nextLong(minLen, medianLen);

                for (long j = 0; j < randomSize; j++) {
                    bf.write(maxTrajectoryID + ", " + timestamp + ", " + ThreadLocalRandom.current().nextLong(minRS, maxRS) + "\n");
                    timestamp = timestamp + 5;
                }
            }
            else{

                writeTrajectory(trajectories.get(i), bf);
            }

            if ((i%1000000)==0) {
                System.out.println("flushed1million");
                bf.flush();
            }

        }

        bf.close();
    }

    public void duplicate() throws IOException {
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
