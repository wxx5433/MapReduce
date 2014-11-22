package jobtracker;

import java.rmi.Remote;
import java.rmi.RemoteException;

import node.NodeID;
import task.MapTask;
import task.ReduceTask;
import task.TaskInProgress;
import tasktracker.HeartBeat;
import tasktracker.TaskTracker;
import job.JobConf;
import job.JobID;
import job.JobInfo;
import job.JobStatus;

public interface JobTrackerService extends Remote {
	
	public void registerTaskTracker(NodeID taskTrackerNodeID) throws RemoteException;
	
	public JobID getJobID() throws RemoteException;
	
	public JobStatus submitJob(JobID jobID, JobConf jobConf) throws RemoteException;
	
	public boolean isJobCompelete(JobID jobID) throws RemoteException;
	
	public boolean isJobFailed(JobID jobID) throws RemoteException;
	
	public boolean isJobKilled(JobID jobID) throws RemoteException;
	
	public void killJob(JobID jobID) throws RemoteException;
	
	public boolean finishMapTasks(JobID jobID) throws RemoteException;
	
	public HeartBeatResponse updateTaskTrackerStatus(HeartBeat heartBeat) throws RemoteException;
	
	public float getMapTasksProgress(JobID jobID) throws RemoteException;
	
	public float getReduceTasksProgress(JobID jobID) throws RemoteException;
	
	public MapTask getNewMapTask(NodeID taskTrackerNodeID) throws RemoteException;
	
	public ReduceTask getNewReduceTask(NodeID taskTrackerNodeID) throws RemoteException;
	
	public JobInfo[] listAllJobs() throws RemoteException;
	
}
