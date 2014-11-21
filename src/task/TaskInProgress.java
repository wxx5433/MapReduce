package task;

import java.util.List;

import fileSplit.FileSplit;

public class TaskInProgress {
	private int jobId;
	private int taskId;
	private FileSplit fileSplit;
	
	public TaskInProgress(int jobId, int taskId, FileSplit split) {
		this.jobId = jobId;
		this.taskId = taskId;
		this.fileSplit = split;
	}
	
	/**
	 * Id within the job. 
	 * It is the index of maps array in JobInProgress.
	 * Passed in when new TaskInProgress
	 * @return
	 */
	public int getTIPId() {
		return this.taskId;
	}
	
	public int getJobId() {
		return this.jobId;
	}
	
	/**
	 * Get where the file splits are
	 * @return
	 */
	public List<String> getSplitLocations() {
		return fileSplit.getHosts();
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (obj instanceof TaskInProgress) {
			TaskInProgress that = (TaskInProgress)obj;
			if (this.jobId == that.jobId && this.taskId == that.taskId) {
				return true;
			}
		}
		return false;
	}

	@Override
	public String toString() {
		return this.jobId + "_" + this.taskId;
	}

	@Override
	public int hashCode() {
		return this.toString().hashCode();
	}

	
}
