package job;

import java.io.IOException;

import job.JobStatus.State;
import tool.Mapper;
import tool.Reducer;
import configuration.Configuration;

public class Job {

	public static enum JobState {
		DEFINE, RUNNING
	};

	private JobState state = JobState.DEFINE;

	private JobClient jobClient;
	private RunningJob info;
	private JobConf jobConf;
	private JobID jobID;

	public static Job getInstance() throws IOException {
		return getInstance(new Configuration());
	}

	public static Job getInstance(Configuration conf) throws IOException {
		return new Job(conf);
	}

	public static Job getInstance(Configuration conf, String jobName)
			throws IOException {
		Job result = getInstance(conf);
		result.setJobName(jobName);
		return result;
	}

	public Job() throws IOException {
		this(new Configuration());
	}

	public Job(Configuration conf) {
		// new jobConf, pass configuration to jobConf
		jobConf = new JobConf(conf);
	}

	public Job(Configuration conf, String jobName) {
		this(conf);
		setJobName(jobName);
	}

	JobClient getJobClient() {
		return jobClient;
	}

	public void setNumReduceTasks(int tasks) throws IllegalStateException {
		jobConf.setNumReduceTasks(tasks);
	}

	public void setMapperClass(Class<? extends Mapper> cls)
			throws IllegalStateException {
		jobConf.setMapperClass(cls);
	}

	public void setReducerClass(Class<? extends Reducer> cls)
			throws IllegalStateException {
		jobConf.setReducerClass(cls);
	}

	public void setJobName(String name) throws IllegalStateException {
		jobConf.setJobName(name);
	}

	public float mapProgress() throws IOException {
		return info.mapProgress();
	}

	public float reduceProgress() throws IOException {
		return info.reduceProgress();
	}

	public boolean isComplete() throws IOException {
		return info.isComplete();
	}

	public void killJob() throws IOException {
		info.killJob();
	}

	/**
	 * Submit the job to the cluster.
	 * 
	 * @throws IOException
	 */
	public void submit() throws IOException, InterruptedException,
			ClassNotFoundException {

		// Connect to the JobTracker and submit the job
		connect();
		info = jobClient.submitJobInternal(jobConf);
		info.setJobState(State.RUNNING);
		setJobID(info.getJobID());
		state = JobState.RUNNING;
	}

	private void setJobID(JobID id) {
		this.jobID = id;
	}

	public JobID getJobID() {
		return jobID;
	}

	/**
	 * Open a connection to the JobTracker
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void connect() throws IOException, InterruptedException {
		jobClient = new JobClient((JobConf) getConfiguration());
	}

	private JobConf getConfiguration() {
		return jobConf;
	}

	/**
	 * Submit the job to the cluster and wait for it to finish.
	 * 
	 * @param verbose
	 *            print the progress to the user
	 * @return true if the job succeeded
	 * @throws IOException
	 *             thrown if the communication with the <code>JobTracker</code>
	 *             is lost
	 */
	public boolean waitForCompletion(boolean verbose) throws IOException,
			InterruptedException, ClassNotFoundException {
		if (state == JobState.DEFINE) {
			submit();
		}
		info.waitForCompletion();
		return isComplete();
	}

}
