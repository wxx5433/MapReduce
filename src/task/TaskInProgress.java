package task;

public class TaskInProgress {
	private int id;
	
	/**
	 * Id within the job. 
	 * It is the index of maps array in JobInProgress.
	 * Passed in when new TaskInProgress
	 * @return
	 */
	public int getTIPId() {
		return this.id;
	}
}
