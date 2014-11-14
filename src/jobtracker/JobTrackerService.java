package jobtracker;

import java.rmi.Remote;

import job.JobID;

public interface JobTrackerService extends Remote {
	public JobID getJobID();
	
	public void submitJobToJobTracker();
	
	public boolean finishMapTasks();
	
	public HeartBeatResponse updateTaskTrackerStatus();
	
	public double getMapTasksProgress();
	
	public double getReduceTasksProgress();
}
