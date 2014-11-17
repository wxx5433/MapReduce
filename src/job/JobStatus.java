package job;

public class JobStatus {

	private int jobID;
	private float mapProgress;
	private float reduceProgress;

	/**
	 * Current state of the job 
	 */
	public static enum State {
		RUNNING(1),
		SUCCEEDED(2),
		FAILED(3),
		PREP(4),
		KILLED(5);

		int value;

		State(int value) {
			this.value = value;
		}

		public int getValue() {
			return value;
		}
	}

	public int getJobID() {
		return jobID;
	}

	public void setJobID(int jobID) {
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
}
