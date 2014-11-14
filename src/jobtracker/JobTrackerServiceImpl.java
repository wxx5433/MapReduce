package jobtracker;

import java.util.Map;
import java.util.Queue;

import job.Job;
import job.JobID;
import job.JobInProgress;
import nameNode.NameNodeService;
import node.NodeID;
import task.MapTask;
import task.ReduceTask;

public class JobTrackerServiceImpl implements JobTrackerService {

	@Override
	public JobID getJobID() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void submitJobToJobTracker() {
		// TODO Auto-generated method stub
		
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
