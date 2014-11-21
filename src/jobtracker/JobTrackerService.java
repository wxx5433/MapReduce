package jobtracker;

import java.rmi.Remote;
import java.rmi.RemoteException;

import task.TaskInProgress;
import tasktracker.HeartBeat;
import tasktracker.TaskTracker;
import job.JobConf;
import job.JobID;
import job.JobStatus;

public interface JobTrackerService extends Remote {
	
	public void registerTaskTracker(TaskTracker taskTracker) throws RemoteException;
	
	public JobID getJobID() throws RemoteException;
	
	public JobStatus submitJob(JobID jobID, JobConf jobConf) throws RemoteException;
	
	public boolean isJobCompelete(JobID jobID) throws RemoteException;
	
	public boolean finishMapTasks(JobID jobID) throws RemoteException;
	
	public HeartBeatResponse updateTaskTrackerStatus(HeartBeat heartBeat) throws RemoteException;
	
	public float getMapTasksProgress(JobID jobID) throws RemoteException;
	
	public float getReduceTasksProgress(JobID jobID) throws RemoteException;
	
	public TaskInProgress getNewMapTask(TaskTracker tt) throws RemoteException;
	
	public TaskInProgress getNewReduceTask(TaskTracker tt) throws RemoteException;
	
}
