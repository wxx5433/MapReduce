package job;

import java.io.Serializable;

public class JobStatus implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 526973507263427352L;
	private JobID jobID;
	private JobConf conf;
	private float mapProgress;
	private float reduceProgress;
	private State state;

	public JobStatus(JobID jobID, JobConf conf) {
		this.jobID = jobID;
		this.conf = conf;
		state = State.PREP;
	}

	/**
	 * Current state of the job
	 */
	public static enum State {
		RUNNING(1), COMPLETED(2), FAILED(3), PREP(4), KILLED(5);

		int value;

		State(int value) {
			this.value = value;
		}

		public int getValue() {
			return value;
		}
	}

	public JobID getJobID() {
		return jobID;
	}

	public void setJobID(JobID jobID) {
		this.jobID = jobID;
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

	public void setComplete() {
		state = State.COMPLETED;
	}

	public boolean isComplete() {
		return state == State.COMPLETED;
	}

	public int getState() {
		return state.value;
	}

	public void setState(State s) {
		state = s;
	}
}
