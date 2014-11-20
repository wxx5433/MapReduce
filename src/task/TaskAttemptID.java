package task;

import job.JobID;

public class TaskAttemptID {

	private int taskId;
	private int attemptID;
	private JobID jobID;
	private boolean mapper;

	public TaskAttemptID(JobID jobID, int mapTaskNum, int attemptID) {
		this.jobID = jobID;
		this.taskId = mapTaskNum;
		this.attemptID = attemptID;
	}

	public int getTaskID() {
		return taskId;
	}

	public void setTaskID(int taskNum) {
		this.taskId = taskNum;
	}

	public int getAttemptID() {
		return attemptID;
	}

	public void setAttemptID(int attemptID) {
		this.attemptID = attemptID;
	}

	public JobID getJobID() {
		return jobID;
	}

	public void setJobID(JobID jobID) {
		this.jobID = jobID;
	}

	public String toString() {
		if (mapper)
			return jobID.toString() + "_" + "maptask_" + taskId + "attempt_"
					+ attemptID;
		else
			return jobID.toString() + "_" + "reducetask_" + taskId
					+ "attempt_" + attemptID;
	}

}
