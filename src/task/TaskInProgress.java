package task;

import java.util.List;

import job.JobID;
import fileSplit.FileSplit;

public class TaskInProgress {
//	private JobID jobId;
//	private int taskId;
	private TaskAttemptID taskAttemptID;
	private FileSplit fileSplit;
	private boolean isMapper;
	
	public TaskInProgress(JobID jobId, int taskId, boolean isMapper) {
//		this.jobId = jobId;
//		this.taskId = taskId;
		// initially the taskAttempNum = 1
		this.taskAttemptID = new TaskAttemptID(jobId, taskId, 1);
		this.isMapper = isMapper;
	}
	
	public TaskInProgress(JobID jobId, int taskId, FileSplit split, boolean isMapper) {
		this(jobId, taskId, isMapper);
		this.fileSplit = split;
	}
	
	
	/**
	 * Id within the job. 
	 * It is the index of maps array in JobInProgress.
	 * Passed in when new TaskInProgress
	 * @return
	 */
	public int getTIPId() {
		return this.taskAttemptID.getTaskID();
	}
	
	public JobID getJobId() {
		return this.taskAttemptID.getJobID();
	}
	
	public void increaseTaskAttemptNum() {
		taskAttemptID.setAttemptNum(getTaskAttemptNum() + 1);
	}
	
	public int getTaskAttemptNum() {
		return taskAttemptID.getAttemptNum();
	}
	
	/**
	 * Get where the file splits are
	 * @return
	 */
	public List<String> getSplitLocations() {
		return fileSplit.getHosts();
	}
	
	public boolean isMapper() {
		return this.isMapper;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (obj instanceof TaskInProgress) {
			TaskInProgress that = (TaskInProgress)obj;
			if (this.taskAttemptID == that.taskAttemptID) {
				return true;
			}
		}
		return false;
	}

	@Override
	public String toString() {
		return this.taskAttemptID.toString();
	}

	@Override
	public int hashCode() {
		return this.toString().hashCode();
	}

	
}
