package job;

import java.io.Serializable;

public class JobInfo implements Serializable {

	private static final long serialVersionUID = 799288600636556291L;
	
	private String jobName;
	private int jobId;
	private int numMapTasks;
	private int numReduceTasks;
	private float mapProgress;
	private float reduceProgress;
	
	public JobInfo(String jobName, int jobId, 
			int numMapTasks, int numReduceTasks, 
			float mapProgress, float reduceProgress) {
		this.jobName = jobName;
		this.jobId = jobId;
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

	public int getJobId() {
		return jobId;
	}

	public void setJobId(int jobId) {
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

}
