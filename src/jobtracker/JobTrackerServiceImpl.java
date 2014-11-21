package jobtracker;

import java.rmi.RemoteException;
import java.util.Queue;

import node.NodeID;
import task.MapTask;
import task.ReduceTask;
import task.TaskInProgress;
import tasktracker.HeartBeat;
import tasktracker.TaskTracker;
import job.JobConf;
import job.JobID;
import job.JobStatus;

public class JobTrackerServiceImpl implements JobTrackerService {

	private JobTracker jobTracker;

	public JobTrackerServiceImpl(JobTracker jobTracker) {
		super();
		this.jobTracker = jobTracker;
	}

	@Override
	public JobID getJobID() throws RemoteException {
		return new JobID(jobTracker.getNewJobID());
	}

	/**
	 * Return JobStatus information to JobClient to get job status information
	 */
	@Override
	public JobStatus submitJob(JobID jobID, JobConf jobConf)
			throws RemoteException {
		JobStatus jobStatus = new JobStatus(jobID, jobConf);
		jobTracker.addJob(jobID, jobConf);
		return jobStatus;
	}

	@Override
	public boolean finishMapTasks(JobID jobID) throws RemoteException {
		return jobTracker.getFinishedMapTasksNum(jobID) == jobTracker
				.getMapTasksNum(jobID);
	}

	@Override
	public HeartBeatResponse updateTaskTrackerStatus(HeartBeat heartBeat)
			throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public float getMapTasksProgress(JobID jobID) throws RemoteException {
		return (float) jobTracker.getFinishedMapTasksNum(jobID)
				/ (float) jobTracker.getMapTasksNum(jobID);
	}

	@Override
	public float getReduceTasksProgress(JobID jobID) throws RemoteException {
		return (float) jobTracker.getFinishedReduceTasksNum(jobID)
				/ (float) jobTracker.getReduceTasksNum(jobID);
	}

	@Override
	public void registerTaskTracker(NodeID taskTrackerNodeID)
			throws RemoteException {
		jobTracker.addTaskTracker(taskTrackerNodeID);
	}

	@Override
	public MapTask getNewMapTask(NodeID taskTrackerNodeID) 
			throws RemoteException {
		return jobTracker.getNewMapTask(taskTrackerNodeID);
	}

	@Override
	public ReduceTask getNewReduceTask(NodeID taskTrackerID) throws RemoteException {
		return jobTracker.getNewReduceTask(taskTrackerID);
	}

	@Override
	public boolean isJobCompelete(JobID jobID) throws RemoteException {
		return jobTracker.isJobComplete(jobID);
	}

}
