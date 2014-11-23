package job;

import java.io.IOException;
import java.util.Map;

import configuration.Configuration;
import job.JobStatus.State;
import jobtracker.JobTrackerService;
import node.NodeID;
import task.MapTask;
import task.TaskAttemptID;

public class RunningJob implements RunningJobInterface {
	
	private JobID jobID;
	private JobStatus jobStatus;
	private JobTrackerService jobTrackerService;
	
	public RunningJob(JobClient jobClient, JobStatus jobStatus) {
		this.jobStatus = jobStatus;
		this.jobID = jobStatus.getJobID();
		this.jobTrackerService = jobClient.getJobTrackerService();
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
		Configuration configuration = new Configuration();
		boolean isJobComplete =  false;
		boolean isJobFailed = false;
		boolean isJobKilled = false;
		while (!(isJobComplete = jobTrackerService.isJobCompelete(jobID))
				&& !(isJobFailed = jobTrackerService.isJobFailed(jobID))
				&& !(isJobKilled = jobTrackerService.isJobKilled(jobID))) {
			try {
				Thread.sleep(configuration.heartBeatInterval);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
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
		if (isJobComplete) {
			setJobState(State.COMPLETED);
			System.out.println("Job complete: " + jobID);
		} else if (isJobFailed) {
			setJobState(State.FAILED);
			System.out.println("Job failed: " + jobID);
		} else if (isJobKilled) {
			setJobState(State.KILLED);
			System.out.println("Job killed: " + jobID);
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


}
