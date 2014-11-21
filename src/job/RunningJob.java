package job;

import java.io.IOException;
import java.util.Map;

import job.JobStatus.State;
import jobtracker.JobTrackerService;
import node.NodeID;
import task.MapTask;
import task.TaskAttemptID;

public class RunningJob implements RunningJobInterface {
	
	private JobID jobID;
	private JobStatus jobStatus;
	private JobTrackerService jobTrackerService;
	
	public RunningJob(JobStatus jobStatus) {
		this.jobStatus = jobStatus;
		this.jobID = jobStatus.getJobID();
		this.jobTrackerService = JobClient.getJobTrackerService();
	}
	
	@Override
	public JobID getJobID() {
		return this.jobID;
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
	public void setMapProgress() throws IOException {
		jobStatus.setMapProgress(jobTrackerService.getMapTasksProgress(jobID));
	}

	@Override
	public void setReduceProgress() throws IOException {
		jobStatus.setReduceProgress(jobTrackerService.getReduceTasksProgress(jobID));
	}

	@Override
	public float mapProgress() throws IOException {
		return jobStatus.getMapProgress();
	}

	@Override
	public float reduceProgress() throws IOException {
		return jobStatus.getReduceProgress();
	}

	@Override
	public boolean isComplete() throws IOException {
		return jobStatus.isComplete();
	}

	@Override
	public void waitForCompletion() throws IOException {
		// ask the jobTracker whether the job has complete
		while (!jobTrackerService.isJobCompelete(jobID)) {
//			Thread.sleep(// heart beat interval here);
			StringBuilder sb = new StringBuilder();
			sb.append("map: ");
			sb.append(jobTrackerService.getMapTasksProgress(jobID));
			sb.append("%");
			sb.append(", ");
			sb.append("reduce: ");
			sb.append(jobTrackerService.getReduceTasksProgress(jobID));
			sb.append("%\n");
			System.out.println(sb.toString());
		}
	}

	@Override
	public int getJobState() throws IOException {
		return jobStatus.getState();
	}
	
	@Override
	public void setJobState(State s) throws IOException {
		jobStatus.setState(s);
	}

	@Override
	public JobStatus getJobStatus() throws IOException {
		return jobStatus;
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
