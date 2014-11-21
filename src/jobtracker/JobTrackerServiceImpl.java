package jobtracker;

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
	public JobID getJobID() {
		return new JobID(jobTracker.getNewJobID());
	}

	/**
	 * Return JobStatus information to JobClient to get job status information
	 */
	@Override
	public JobStatus submitJob(JobID jobID, JobConf jobConf) {
		JobStatus jobStatus = new JobStatus(jobID, jobConf);
		jobTracker.addJob(jobID, jobConf);
		return jobStatus;
	}

	@Override
	public boolean finishMapTasks(JobID jobID) {
		return jobTracker.getFinishedMapTasksNum(jobID) 
					== jobTracker.getMapTasksNum(jobID);
	}

	@Override
	public HeartBeatResponse updateTaskTrackerStatus() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public float getMapTasksProgress(JobID jobID) {
		return (float)jobTracker.getFinishedMapTasksNum(jobID) /
					(float)jobTracker.getMapTasksNum(jobID);
	}

	@Override
	public float getReduceTasksProgress(JobID jobID) {
		return (float)jobTracker.getFinishedReduceTasksNum(jobID) /
					(float)jobTracker.getReduceTasksNum(jobID);
	}

	@Override
	public void registerTaskTracker(TaskTracker taskTracker) {
		jobTracker.addTaskTracker(taskTracker);
	}

	@Override
	public TaskInProgress getNewMapTask(TaskTracker tt) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TaskInProgress getNewReduceTask(TaskTracker tt) {
		// TODO Auto-generated method stub
		return null;
	}

}
