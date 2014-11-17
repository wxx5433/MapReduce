package jobtracker;

import java.rmi.Remote;

import tasktracker.TaskTracker;
import job.JobConf;
import job.JobID;
import job.JobStatus;

public interface JobTrackerService extends Remote {
	
	public void registerTaskTracker(TaskTracker taskTracker);
	
	public JobID getJobID();
	
	public JobStatus submitJob(JobID jobID, JobConf jobConf);
	
	public boolean finishMapTasks(JobID jobID);
	
	public HeartBeatResponse updateTaskTrackerStatus();
	
	public float getMapTasksProgress(JobID jobID);
	
	public float getReduceTasksProgress(JobID jobID);
	
}
