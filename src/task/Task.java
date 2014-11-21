package task;

import java.io.IOException;
import java.io.Serializable;

import job.JobConf;
import job.JobID;

public interface Task extends Serializable {

	public Task setInputFile(String fileName);

	public Task setOutputFile(String fileName);

	public Task setJobConf(JobConf jobConf);

	public JobID getJobID();

	public JobConf getJobConf();

	public Task setTaskID(TaskAttemptID id);

	public String getOutputFile();

	public String getInputFile();

	public TaskAttemptID getTaskID();

	void run() throws ClassNotFoundException, InstantiationException,
			IOException, InterruptedException, IllegalAccessException;

}
