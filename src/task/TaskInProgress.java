package task;

import java.util.List;

import fileSplit.FileSplit;

public class TaskInProgress {
	private int id;
	private FileSplit fileSplit;
	
	/**
	 * Id within the job. 
	 * It is the index of maps array in JobInProgress.
	 * Passed in when new TaskInProgress
	 * @return
	 */
	public int getTIPId() {
		return this.id;
	}
	
	/**
	 * Get where the file splits are
	 * @return
	 */
	public List<String> getSplitLocations() {
		return fileSplit.getHosts();
	}
}
