package jobtracker;

import java.rmi.Remote;

import job.JobID;
import job.JobStatus;

public interface JobTrackerService extends Remote {
	public JobID getJobID();
	
	public JobStatus submitJob();
	
	public boolean finishMapTasks();
	
	public HeartBeatResponse updateTaskTrackerStatus();
	
	public double getMapTasksProgress();
	
	public double getReduceTasksProgress();
}
