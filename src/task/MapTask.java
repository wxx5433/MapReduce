package task;

import inputformat.InputFormat;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;

import job.JobConf;
import job.JobID;
import partitioner.Partitioner;
import task.MapOutputCollector.CollectorContext;
import tool.MapContext;
import tool.MapContextImpl;
import tool.Mapper;
import tool.WrappedMapper;
import fileSplit.MapInputSplit;

public class MapTask implements Task {
	/**
	 * 
	 */
	private static final long serialVersionUID = -1690738592028057018L;
	private MapInputSplit mapInputSplit;
	private String outputFilePath;
	private JobConf jobConf;
	private TaskAttemptID taskAttemptID;
	private ArrayList<String> outputPathsForReduce;

	public MapTask(MapInputSplit mapInputSplit, String outputFilePath,
			JobConf jobConf, TaskAttemptID taskAttemptID) {
		this.mapInputSplit = mapInputSplit;
		this.jobConf = jobConf;
		this.taskAttemptID = taskAttemptID;
		this.outputFilePath = outputFilePath;
	}

	public MapTask setOutputFile(String outputFilePath) {
		this.outputFilePath = outputFilePath;
		return this;
	}

	public ArrayList<String> getOutputPathsForReduce() {
		return outputPathsForReduce;
	}

	public void setOutputPathsForReduce(ArrayList<String> outputPathsForReduce) {
		this.outputPathsForReduce = outputPathsForReduce;
	}

	public MapTask setJobConf(JobConf jobConf) {
		this.jobConf = jobConf;
		return this;
	}

	public MapTask setTaskID(TaskAttemptID taskAttemptID) {
		this.taskAttemptID = taskAttemptID;
		return this;
	}

	public String getOutputFile() {
		return outputFilePath;
	}

	@Override
	public JobConf getJobConf() {
		return jobConf;
	}

	public TaskAttemptID getTaskID() {
		return taskAttemptID;
	}

	@Override
	public void run() throws ClassNotFoundException, InstantiationException,
			IOException, InterruptedException, IllegalAccessException {
		runMapper(jobConf);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private <INKEY, INVALUE, OUTKEY, OUTVALUE> void runMapper(JobConf jobConf)
			throws IOException, InterruptedException, ClassNotFoundException,
			InstantiationException, IllegalAccessException {

		// make a mapper
		System.out.println("mapperClassName: " + jobConf.getMapperClass());
		Class<?> mapperClass = Class.forName(jobConf.getMapperClass());
		Mapper mapper = (Mapper) mapperClass.newInstance();

		// rebuild the input format
		Class<?> inputFormatClass = Class.forName(jobConf.getInputFormat());
		InputFormat inputFormat = (InputFormat) inputFormatClass.newInstance();

		RecordReader input = inputFormat
				.getRecordReader(mapInputSplit, jobConf);

		NewOutputCollector outputCollector = new NewOutputCollector(this,
				jobConf, outputFilePath);

		RecordWriter output = outputCollector;

		MapContext<INKEY, INVALUE, OUTKEY, OUTVALUE> mapContext = new MapContextImpl<INKEY, INVALUE, OUTKEY, OUTVALUE>(
				getTaskID(), input, output, mapInputSplit);

		Mapper<INKEY, INVALUE, OUTKEY, OUTVALUE>.Context mapperContext = new WrappedMapper<INKEY, INVALUE, OUTKEY, OUTVALUE>()
				.getMapContext(mapContext);

		mapper.run(mapperContext);
		statusUpdate();
		input.close();
		output.close();
		setOutputPathsForReduce(outputCollector.getOutputPaths());
	}

	private void statusUpdate() {
	}

	@Override
	public Task setInputFile(String fileName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getInputFile() {
		// TODO Auto-generated method stub
		return null;
	}

	private class NewOutputCollector<K, V> extends RecordWriter<K, V> {
		private MapOutputCollector<K, V> collector;
		private Partitioner<K, V> partitioner;
		private int partitions;
		private MapTask mapTask;
		private ArrayList<String> outputPaths;

		@SuppressWarnings("unchecked")
		public NewOutputCollector(MapTask maptask, JobConf jobConf,
				String outputFilePath) throws IOException,
				ClassNotFoundException, InstantiationException,
				IllegalAccessException {
			this.mapTask = maptask;
			collector = createCollector(mapTask, jobConf, outputFilePath);
			partitions = jobConf.getNumReduceTasks();
			if (partitions > 0) {
				Class<?> partitionerClass = Class.forName(jobConf
						.getPartitionerClass());
				partitioner = (Partitioner<K, V>) partitionerClass
						.newInstance();
			} else {
				partitioner = new Partitioner<K, V>() {
					@Override
					public int getPartition(K key, V value, int numPartitions) {
						return -1;
					}
				};
			}
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		private MapOutputCollector<K, V> createCollector(MapTask mapTask,
				JobConf jobConf, String outputFilePath)
				throws FileNotFoundException {
			MapOutputCollectorImpl collector = new MapOutputCollectorImpl(
					outputFilePath);
			CollectorContext collectorContext = new CollectorContext(mapTask,
					jobConf);
			collector.init(collectorContext);
			outputPaths = collector.getOutputPaths();
			return collector;
		}

		@Override
		public void write(K key, V value) throws IOException {
			collector.collect(key, value,
					partitioner.getPartition(key, value, partitions));
		}

		@Override
		public void close() throws IOException {
			collector.close();
		}

		public ArrayList<String> getOutputPaths() {
			return outputPaths;
		}

	}

	@Override
	public JobID getJobID() {
		return jobConf.getJobID();
	}

}
