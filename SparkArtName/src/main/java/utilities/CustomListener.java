package utilities;

import org.apache.spark.scheduler.*;
import scala.Unit;

/**
 * Created by giannis on 26/02/19.
 */
public class CustomListener extends SparkListener {

    private long stageStartTime;
    private long stageEndTime;
    private long taskStartTime;
    private long taskEndTime;
    private static long sumTaskTime;


    @Override
    public void onStageSubmitted(SparkListenerStageSubmitted stageSubmitted){
        stageStartTime=System.nanoTime();
    }



    @Override
    public void onStageCompleted(SparkListenerStageCompleted stageCompleted){

//        stageEndTime=System.nanoTime();
//
//        System.out.println(stageCompleted.stageInfo().name());
//        System.out.println("diskBytesSpilled():"+stageCompleted.stageInfo().taskMetrics().diskBytesSpilled());
//        System.out.println("executorCpuTime((including fetching shuffle data))(ns):"+stageCompleted.stageInfo().taskMetrics().executorCpuTime());
//        System.out.println("executorDeserializeCpuTime(ns):"+stageCompleted.stageInfo().taskMetrics().executorDeserializeCpuTime());
//        System.out.println("executorDeserializeTime():"+stageCompleted.stageInfo().taskMetrics().executorDeserializeTime());
//        System.out.println("executorRunTime():"+stageCompleted.stageInfo().taskMetrics().executorRunTime());
//        System.out.println("jvmGCTime():"+stageCompleted.stageInfo().taskMetrics().jvmGCTime());
//        System.out.println("memoryBytesSpilled():"+stageCompleted.stageInfo().taskMetrics().memoryBytesSpilled());
//        System.out.println("resultSerializationTime():"+stageCompleted.stageInfo().taskMetrics().resultSerializationTime());
//
//        long sum=stageCompleted.stageInfo().taskMetrics().executorDeserializeCpuTime() + stageCompleted.stageInfo().taskMetrics().executorDeserializeTime() + stageCompleted.stageInfo().taskMetrics().executorCpuTime() + stageCompleted.stageInfo().taskMetrics().jvmGCTime() + stageCompleted.stageInfo().taskMetrics().resultSerializationTime();
//        System.out.println("executorDeserializeCpuTime() + executorDeserializeTime() + executorCpuTime() + jvmGCTime() + resultSerializationTime():"+sum);
//        System.out.println("MyMesauredTime (ns)"+ (stageEndTime-stageStartTime));
    }

    @Override
    public void onTaskStart(SparkListenerTaskStart taskStart){

        taskStartTime=System.nanoTime();

    }


    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd){

        taskEndTime=System.nanoTime();

//        System.out.println("taskEnd.taskInfo().taskId():"+taskEnd.taskInfo().taskId()+" running on:"+ taskEnd.taskInfo().host());
//        System.out.println("finishTime() - launchTime():"+(taskEnd.taskInfo().finishTime()-taskEnd.taskInfo().launchTime()));
//        System.out.println("executorRunTime():"+taskEnd.taskMetrics().executorRunTime());
        System.out.println("executorCpuTime():"+taskEnd.taskMetrics().executorCpuTime());
//        System.out.println("resultSerializationTime():"+taskEnd.taskMetrics().time);
//        System.out.println("resultSerializationTime():"+taskEnd.taskMetrics().time);
//        System.out.println("executorCpuTime():"+taskEnd.taskMetrics().);
        long diff=(taskEndTime-taskStartTime);
        System.out.println("taskID:"+taskEnd.taskInfo().taskId());
        sumTaskTime+=diff;
        System.out.println("myTaskElapsedTime():"+diff);
        System.out.println("diff():"+(taskEnd.taskMetrics().executorCpuTime()-diff));
    }

    @Override
    public void onJobEnd(SparkListenerJobEnd jobEnd){
        System.out.println("onJE:sumTaskTime:"+sumTaskTime);
    }

    @Override
    public void onApplicationEnd(SparkListenerApplicationEnd jobEnd){
        System.out.println("onAE:sumTaskTime:"+sumTaskTime);
    }


}
