package jobtracker;

import job.JobID;
import job.JobStatus;

public class JobTrackerServiceImpl implements JobTrackerService {

	@Override
	public JobID getJobID() {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * Return JobStatus information to JobClient to get job status information
	 */
	@Override
	public JobStatus submitJob() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean finishMapTasks() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public HeartBeatResponse updateTaskTrackerStatus() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public double getMapTasksProgress() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double getReduceTasksProgress() {
		// TODO Auto-generated method stub
		return 0;
	}

}
