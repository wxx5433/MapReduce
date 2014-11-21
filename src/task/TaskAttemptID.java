package task;

import java.io.Serializable;

import node.NodeID;
import job.JobID;

public class TaskAttemptID implements Serializable {

	private static final long serialVersionUID = -7756057483718734372L;

	private int taskId;
	private int attemptNum;
	private JobID jobID;
	private boolean mapper;
	private NodeID nodeID;

	public TaskAttemptID(JobID jobID, int mapTaskNum, int attemptNum) {
		this.jobID = jobID;
		this.taskId = mapTaskNum;
		this.attemptNum = attemptNum;
	}

	public int getTaskID() {
		return taskId;
	}

	public void setTaskID(int taskNum) {
		this.taskId = taskNum;
	}

	public int getAttemptID() {
		return attemptNum;
	}

	public void setAttemptID(int attemptID) {
		this.attemptNum = attemptID;
	}

	public NodeID getNodeID() {
		return nodeID;
	}

	public void setNodeID(NodeID nodeID) {
		this.nodeID = nodeID;
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
					+ attemptNum;
		else
			return jobID.toString() + "_" + "reducetask_" + taskId
					+ "attempt_" + attemptNum;
	}

}
