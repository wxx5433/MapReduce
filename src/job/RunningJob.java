package job;

import java.io.IOException;
import java.util.Map;

import node.NodeID;
import task.MapTask;
import task.TaskAttemptID;

public class RunningJob implements RunningJobInterface {
	
	@Override
	public JobID getID() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getJobID() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getJobName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getJobFile() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getTrackingURL() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public float mapProgress() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public float reduceProgress() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public float cleanupProgress() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public float setupProgress() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean isComplete() throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isSuccessful() throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void waitForCompletion() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public int getJobState() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public JobStatus getJobStatus() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void killJob() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void killTask(TaskAttemptID taskId, boolean shouldFail)
			throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void killTask(String taskId, boolean shouldFail) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public String getFailureInfo() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String[] getTaskDiagnostics(TaskAttemptID taskid) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

}
