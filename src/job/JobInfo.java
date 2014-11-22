package job;

import java.io.Serializable;

import job.JobStatus.State;

public class JobInfo implements Serializable {

	private static final long serialVersionUID = 799288600636556291L;

	private String jobName;
	private JobID jobId;
	private int numMapTasks;
	private int numReduceTasks;
	private float mapProgress;
	private float reduceProgress;
	private State state;

	public JobInfo(String jobName, JobID jobID, State state, 
			int numMapTasks, int numReduceTasks, 
			float mapProgress, float reduceProgress) {
		this.jobName = jobName;
		this.jobId = jobID;
		this.state = state;
		this.numMapTasks = numMapTasks;
		this.numReduceTasks = numReduceTasks;
		this.mapProgress = mapProgress;
		this.reduceProgress = reduceProgress;
	}

	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public JobID getJobId() {
		return jobId;
	}

	public void setJobId(JobID jobId) {
		this.jobId = jobId;
	}

	public int getNumMapTasks() {
		return numMapTasks;
	}

	public void setNumMapTasks(int numMapTasks) {
		this.numMapTasks = numMapTasks;
	}

	public int getNumReduceTasks() {
		return numReduceTasks;
	}

	public void setNumReduceTasks(int numReduceTasks) {
		this.numReduceTasks = numReduceTasks;
	}

	public float getMapProgress() {
		return mapProgress;
	}

	public void setMapProgress(float mapProgress) {
		this.mapProgress = mapProgress;
	}

	public float getReduceProgress() {
		return reduceProgress;
	}

	public void setReduceProgress(float reduceProgress) {
		this.reduceProgress = reduceProgress;
	}

	public State getState() {
		return state;
	}

	public void setState(State state) {
		this.state = state;
	}

	private String getStateString() {
		String result = null;
		if (state == State.COMPLETED)  {
			result = "Compelted\n";
		} else if (state == State.FAILED) {
			result = "Failed\n";
		} else if (state == State.KILLED) {
			result = "Killed\n";
		} else {
			result = "Running\n";
		}
		return result;
	}


	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("JobName: " + jobName + ", JobID: " + jobId + ", ");
		sb.append("Status: " + getStateString());
		sb.append("Map tasks num: " + numMapTasks 
				+ ", Reduce tasks num: " + numReduceTasks + "\n");
		sb.append("Map task progress: " + mapProgress 
				+ ", Reduce tasks progress: " + reduceProgress + "\n");
		return new String(sb);
	}
}
