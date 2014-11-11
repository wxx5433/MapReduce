package job;

import java.io.IOException;
import job.JobClient;
import job.JobConf;
import job.JobID;
import job.JobStatus;
import job.RunningJob;
import tool.Mapper;
import tool.Reducer;
import configuration.ConfigurationStrings;
import configuration.MyConfiguration;

public class Job {

	public static enum JobState {
		DEFINE, RUNNING
	};

	private JobState state = JobState.DEFINE;

	private JobClient jobClient;
	private RunningJob info;
	private JobConf jobConf;
	private JobStatus jobStatus;
	private JobID jobID;

	public static Job getInstance() throws IOException {
		// create with a null Cluster
		return getInstance(new MyConfiguration());
	}

	public static Job getInstance(MyConfiguration conf) throws IOException {
		// create with a null Cluster
		JobConf jobConf = new JobConf(conf);
		return new Job(jobConf);
	}

	public Job() throws IOException {
		this(new MyConfiguration());
	}

	public Job(JobConf jobConf) {
		// TODO Auto-generated constructor stub
	}

	public Job(MyConfiguration myConfiguration) {
		// TODO Auto-generated constructor stub
	}

	JobClient getJobClient() {
		return jobClient;
	}

	public void setNumReduceTasks(int tasks) throws IllegalStateException {
		jobConf.setNumReduceTasks(tasks);
	}

	public void setMapperClass(Class<? extends Mapper> cls)
			throws IllegalStateException {
		jobConf.setMapperClass(ConfigurationStrings.MAPPER_CLASS, cls);
	}

	public void setReducerClass(Class<? extends Reducer> cls)
			throws IllegalStateException {
		jobConf.setReducerClass(ConfigurationStrings.REDUCER_CLASS, cls);
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

	public boolean isSuccessful() throws IOException {
		return info.isSuccessful();
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
		setJobID(info.getID());
		state = JobState.RUNNING;
	}

	private void setJobID(JobID id) {
		this.jobID = id;
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
		return isSuccessful();
	}

}
