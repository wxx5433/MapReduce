package task;

import java.io.IOException;

import job.JobConf;

public interface ShuffleConsumerPlugin {

	/**
	 * To fetch the map outputs.
	 * 
	 * @return true if the fetch was successful; false otherwise.
	 */
	public boolean fetchOutputs() throws IOException;

	/**
	 * close and clean any resource associated with this object.
	 */
	public void close();

	public boolean shuffle();

	public static class Context {
		private JobConf jobConf;
		private ReduceTask reduceTask;

		public Context(JobConf jobConf, ReduceTask reduceTask) {
			this.jobConf = jobConf;
			this.reduceTask = reduceTask;
		}

		public JobConf getJobConf() {
			return jobConf;
		}

		public ReduceTask getReduceTask() {
			return reduceTask;
		}
	}
}
