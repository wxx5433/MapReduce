package jobtracker;

import java.rmi.RemoteException;
import java.util.Queue;

import task.TaskInProgress;
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
	public JobStatus submitJob(JobID jobID, JobConf jobConf) throws RemoteException {
		JobStatus jobStatus = new JobStatus(jobID, jobConf);
		jobTracker.addJob(jobID, jobConf);
		return jobStatus;
	}

	@Override
	public boolean finishMapTasks(JobID jobID) throws RemoteException {
		return jobTracker.getFinishedMapTasksNum(jobID) 
					== jobTracker.getMapTasksNum(jobID);
	}

	@Override
	public HeartBeatResponse updateTaskTrackerStatus() throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public float getMapTasksProgress(JobID jobID) throws RemoteException {
		return (float)jobTracker.getFinishedMapTasksNum(jobID) /
					(float)jobTracker.getMapTasksNum(jobID);
	}

	@Override
	public float getReduceTasksProgress(JobID jobID) throws RemoteException {
		return (float)jobTracker.getFinishedReduceTasksNum(jobID) /
					(float)jobTracker.getReduceTasksNum(jobID);
	}

	@Override
	public void registerTaskTracker(TaskTracker taskTracker) throws RemoteException {
		jobTracker.addTaskTracker(taskTracker);
	}

	@Override
	public TaskInProgress getNewMapTask(TaskTracker tt) throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TaskInProgress getNewReduceTask(TaskTracker tt) throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isJobCompelete(JobID jobID) throws RemoteException {
		return jobTracker.isJobComplete(jobID);
	}

}
