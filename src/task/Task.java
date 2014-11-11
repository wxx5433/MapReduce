package task;

import java.io.Serializable;

import job.Job;
import job.JobConf;

public interface Task extends Serializable {

	public Task setInputFile(String fileName);

	public Task setOutputFile(String fileName);

	public Task setJob(Job job);

	public Job getJob();

	public Task setTaskID(TaskAttemptID id);

	public String getOutputFile();

	public String getInputFile();

	public TaskAttemptID getTaskID();

	void run(JobConf job);

}
