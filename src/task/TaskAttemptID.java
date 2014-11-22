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

	public TaskAttemptID(JobID jobID, int taskId, int attemptNum,
			boolean mapper) {
		this.jobID = jobID;
		this.taskId = taskId;
		this.attemptNum = attemptNum;
		this.mapper = mapper;
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
		if (this.mapper != compare.mapper) {
			return false;
		}
		if (this.taskId != compare.taskId) {
			return false;
		}
		if (this.attemptNum != compare.attemptNum) {
			return false;
		}
		return true;
	}

	@Override
	public int hashCode() {
		return toString().hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj instanceof TaskAttemptID) {
			TaskAttemptID that = (TaskAttemptID)obj;
			if (this.compareTo(that)) {
				return true;
			}
		}
		return false;
	}

}
