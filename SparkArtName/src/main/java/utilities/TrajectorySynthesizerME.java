package utilities;

import comparators.IntegerComparator;
import comparators.LongComparator;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import map.functions.CSVRecordToTrajectory;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import partitioning.methods.TimeSlicing;
import projections.ProjectRoadSegments;
import projections.ProjectTimestamps;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
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
        JavaPairRDD<Integer, Iterable<CSVRecord>> recordsCached = records.groupBy(csv -> csv.getTrajID()).cache();


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
        final long fixedRandomTimestamp = ThreadLocalRandom.current().nextLong(minTimestamp, maxTimestamp);

        trajectories.coalesce(1).foreach(new VoidFunction<Trajectory>() {
            int counter = 0;


            @Override
            public void call(Trajectory t) throws Exception {
                ArrayList<Long> timeDiffs = getTimeDiffs(t.getTimestamps());
                int j;
                long randomStartTime;

                boolean skewed=false;
                if (counter % 2 == 0) {
                    randomStartTime = fixedRandomTimestamp;
                    skewed=true;
                } else {
                    randomStartTime = ThreadLocalRandom.current().nextLong(minTimestamp, maxTimestamp);
                }

                for (j = 0; j < t.getRoadSegments().size() - 1; j++) {
                    t.setTimestampAt(j, randomStartTime);
                    randomStartTime += timeDiffs.get(j);
                }
                t.setTimestampAt(j, randomStartTime);

//                if (skewed){
//                    List<Long> timePeriods = Intervals.getIntervals(100, minTimestamp, maxTimestamp);
//                    System.out.println("skewed tarjectory belongs to:"+Trajectory.determineTimeSlice(t,timePeriods));
//                }
//                else{
//                    List<Long> timePeriods = Intervals.getIntervals(100, minTimestamp, maxTimestamp);
//                    System.out.println("normal tarjectory belongs to:"+Trajectory.determineTimeSlice(t,timePeriods));
//                }

                ++counter;

            }
        });


        trajectories.saveAsTextFile("timeSkewedDataset");

    }

    public void roadSegmentSkewedDataset(JavaSparkContext sc) throws IOException {


        Random random = new Random();
        final int randomStartingRS = trajectories.takeSample(false, 1).get(0).getStartingRS();

        final JavaRDD<Trajectory> randomTrajs = trajectories.filter(t -> t.getStartingRS() == randomStartingRS).cache();

        Broadcast<List<Trajectory>> broadcastedrandomTrajs = sc.broadcast(randomTrajs.collect());
        System.out.println("Chose starting RS:" + randomStartingRS + " with frequency:" + randomTrajs.count());
        trajectories.coalesce(1).foreach(new VoidFunction<Trajectory>() {
            int counter = 0;

            @Override
            public void call(Trajectory t) throws Exception {
                List<Trajectory> broadTrajs = broadcastedrandomTrajs.value();
                Trajectory tr = broadTrajs.get(random.nextInt(broadTrajs.size() - 1));
                if (counter % 10 == 0) {


                    if (t.getRoadSegments().size() < tr.getRoadSegments().size()) {
                        t.setRoadSegments(tr.getRoadSegments());
                        //also copy the timestamps
                        t.setTimestamps(tr.getTimestamps());
                    } else {
                        t.setRoadSegments(tr.getRoadSegments());
                        //leave timestamps unchanged but trim them
                        t.trimTimestamps(tr.getTimestamps().size());
                    }

                }
                ++counter;

            }
        });

        trajectories.saveAsTextFile("roadSegmentSkewedDataset");
        System.exit(1);

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
