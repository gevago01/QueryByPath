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

        for (Integer ts:timeSlices){
            //System.out.println("trajectory:"+mo.getTrajectoryID()+" assignedto:"+ts);
            Trajectory traj=new Trajectory(trajectory,ts);
            traj.setTimeSlice(ts);
            list.add(traj);
        }


        return list;
    }
}
