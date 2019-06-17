package map.functions;

import org.apache.spark.api.java.function.Function;
import utilities.Trajectory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by giannis on 11/12/18.
 */
public class TrajToTSTrajs implements Function<Trajectory, Iterable<Trajectory>> {
    private List<Long> timePeriods;


    public TrajToTSTrajs(List<Long> timeIntervals) {
        timePeriods=timeIntervals;
    }

    @Override
    public Iterable<Trajectory> call(Trajectory trajectory) throws Exception {

        List<Trajectory> list=new ArrayList<>();
        List<Integer> timeSlices = trajectory.determineTimeSlices(timePeriods);

        //if a trajectory belongs to multiple time slices duplicate
        //the trajectory as many times as the number of time slices
        for (Integer ts:timeSlices){
            Trajectory traj=new Trajectory(trajectory,ts);
            traj.setTimestamps(trajectory.getTimestamps());
            traj.setPartitionID(ts);
            list.add(traj);
        }


        return list;
    }
}
