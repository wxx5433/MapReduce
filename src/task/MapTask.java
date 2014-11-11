package task;

import job.Job;
import job.JobConf;

public class MapTask implements Task {
	private String inputFilePath;
	private String outputFilePath;
	private Job job;
	private TaskAttemptID taskAttemptID;

	public MapTask() {
	}

	public MapTask(String inputFilePath, String outputFilePath, Job job,
			TaskAttemptID taskAttemptID) {
		this.inputFilePath = inputFilePath;
		this.outputFilePath = outputFilePath;
		this.job = job;
		this.taskAttemptID = taskAttemptID;
	}

	public MapTask setInputFile(String inputFilePath) {
		this.inputFilePath = inputFilePath;
		return this;
	}

	public MapTask setOutputFile(String outputFilePath) {
		this.outputFilePath = outputFilePath;
		return this;
	}

	public MapTask setJob(Job job) {
		this.job = job;
		return this;
	}

	public MapTask setTaskID(TaskAttemptID taskAttemptID) {
		this.taskAttemptID = taskAttemptID;
		return this;
	}

	public String getInputFile() {
		return inputFilePath;
	}

	public String getOutputFile() {
		return outputFilePath;
	}

	@Override
	public Job getJob() {
		return job;
	}

	public TaskAttemptID getTaskID() {
		return taskAttemptID;
	}

	@Override
	public void run(JobConf jobConf) {
		runMapper(jobConf);
	}

	@SuppressWarnings("unchecked")
	private void runMapper(JobConf jobConf) {

		// make a mapper
		// Class<?> mapper = Class
		// .forName(jobConf.getMapperClass());
		// // rebuild the input split
		// RecordReader reader = new RecordReader(this.inputFilePath,
		// MyConfiguration.splitBlockLinesNum);
		//
		// RecordWriter output = new RecordWriter(this.outputFilePath,
		// MyConfiguration.splitBlockLinesNum);
		//
		// MapContext mapContext = new MapContextImpl<INKEY, INVALUE, OUTKEY,
		// OUTVALUE>(MyConfiguration.getInstance(),
		// jobConf, getTaskID(), input, output, committer, reporter, split);
		// (MyConfiguration conf, TaskAttemptID taskid,
		// RecordReader reader, RecordWriter writer, MyInputSplit split
		// org.apache.hadoop.mapreduce.Mapper<INKEY, INVALUE, OUTKEY,
		// OUTVALUE>.Context mapperContext = new WrappedMapper<INKEY, INVALUE,
		// OUTKEY, OUTVALUE>()
		// .getMapContext(mapContext);
		//
		// input.initialize(split, mapperContext);
		// mapper.run(mapperContext);
		// statusUpdate(umbilical);
		// input.close();
		// output.close(mapperContext);
	}
}
