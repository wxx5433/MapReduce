package task;

import java.io.Serializable;

import job.JobID;
import node.NodeID;

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

	public int getAttemptNum() {
		return attemptNum;
	}

	public void setAttemptNum(int attemptNum) {
		this.attemptNum = attemptNum;
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
			return jobID.toString() + "_" + "reducetask_" + taskId + "attempt_"
					+ attemptNum;
	}

	public boolean compareTo(TaskAttemptID compare) {
		if ((this.jobID.hashCode() == compare.jobID.hashCode())
				&& this.taskId == compare.taskId
				&& this.attemptNum == compare.attemptNum
				&& this.nodeID.equals(compare.nodeID)) {
			return true;
		} else {
			return false;
		}

	}
}
