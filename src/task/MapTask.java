package task;

import java.io.IOException;

import job.Job;
import job.JobConf;
import node.NodeID;
import tool.MapContext;
import tool.MapContextImpl;
import tool.Mapper;
import tool.WrappedMapper;
import configuration.ConfigurationStrings;
import configuration.MyConfiguration;

public class MapTask implements Task {
	private String inputFilePath;
	private String outputFilePath;
	private Job job;
	private TaskAttemptID taskAttemptID;
	private NodeID nodeID;

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

	public NodeID getNodeID() {
		return nodeID;
	}

	public MapTask setNodeID(NodeID nodeID) {
		this.nodeID = nodeID;
		return this;
	}

	@Override
	public void run(JobConf jobConf) throws ClassNotFoundException,
			InstantiationException, IOException, InterruptedException,
			IllegalAccessException {
		runMapper(jobConf);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private <INKEY, INVALUE, OUTKEY, OUTVALUE> void runMapper(JobConf jobConf)
			throws IOException, InterruptedException, ClassNotFoundException,
			InstantiationException, IllegalAccessException {

		// make a mapper
		Class<?> mapperClass = Class.forName(jobConf
				.getMapperClass(ConfigurationStrings.MAPPER_CLASS));

		Mapper mapper = (Mapper) mapperClass.newInstance();
		// rebuild the input split
		
		InputFormat inputFormat = Class.forName(jobConf.get)
		
		RecordReader input = new RecordReader(this.inputFilePath,
				ConfigurationStrings.splitBlockLinesNum);

		RecordWriter output = new RecordWriter(this.outputFilePath,
				ConfigurationStrings.splitBlockLinesNum);

		MapContext<INKEY, INVALUE, OUTKEY, OUTVALUE> mapContext = new MapContextImpl<INKEY, INVALUE, OUTKEY, OUTVALUE>(
				MyConfiguration.getInstance(), getTaskID(), input, output);

		Mapper<INKEY, INVALUE, OUTKEY, OUTVALUE>.Context mapperContext = new WrappedMapper<INKEY, INVALUE, OUTKEY, OUTVALUE>()
				.getMapContext(mapContext);

		input.initialize();
		mapper.run(mapperContext);
		statusUpdate();
		input.close();
		output.close();
	}

	private void statusUpdate() {

	}
}
