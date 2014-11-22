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
import job.JobInProgress;
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
		System.out.println("Receive a heartBeat from: " + heartBeat.getNodeID());
		NodeID taskTrackerNodeID = heartBeat.getNodeID();
		// get all finish tasks
		for (MapTask mapTask: heartBeat.getFinishedMappers()) {
			JobInProgress jip = jobTracker.getJobInProgress(mapTask.getJobID());
			jip.finishMapTask(mapTask);
		}
		for (ReduceTask reduceTask: heartBeat.getFinishedReducers()) {
			JobInProgress jip = jobTracker.getJobInProgress(reduceTask.getJobID());
			jip.finishReduceTask(reduceTask);
		}
		// get all fail tasks
		for (MapTask mapTask: heartBeat.getFailedMappers()) {
			JobInProgress jip = jobTracker.getJobInProgress(mapTask.getJobID());
			jip.failMap(taskTrackerNodeID, mapTask);
		}
		for (ReduceTask reduceTask: heartBeat.getFailedReducers()) {
			JobInProgress jip = jobTracker.getJobInProgress(reduceTask.getJobID());
			jip.failReduce(taskTrackerNodeID, reduceTask);
		}
		
		HeartBeatResponse heartBeatResponse = new HeartBeatResponse();
		for (int i = 0; i < heartBeat.getLeftMapperSlot(); ++i) {
			heartBeatResponse.addNewMapper(jobTracker.getNewMapTask(taskTrackerNodeID));
		}
		for (int i = 0; i < heartBeat.getLeftReducerSlot(); ++i) {
			heartBeatResponse.addNewReducer(jobTracker.getNewReduceTask(taskTrackerNodeID));
		}
		return heartBeatResponse;
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
		System.out.println("New taskTracker online: " + taskTrackerNodeID);
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

	@Override
	public boolean isJobFailed(JobID jobID) throws RemoteException {
		return jobTracker.isJobFailed(jobID);
	}

	@Override
	public boolean isJobKilled(JobID jobID) throws RemoteException {
		return jobTracker.isJobKilled(jobID);
	}

	@Override
	public void killJob(JobID jobID) throws RemoteException {
		jobTracker.killJob(jobID);
	}

}
