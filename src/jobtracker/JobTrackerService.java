package jobtracker;

import java.rmi.Remote;

import job.JobConf;
import job.JobID;
import job.JobStatus;

public interface JobTrackerService extends Remote {
	public JobID getJobID();
	
	public JobStatus submitJob(JobConf jobConf);
	
	public boolean finishMapTasks();
	
	public HeartBeatResponse updateTaskTrackerStatus();
	
	public double getMapTasksProgress();
	
	public double getReduceTasksProgress();
}
