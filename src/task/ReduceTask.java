package task;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import javax.management.RuntimeErrorException;

import job.JobConf;
import job.JobID;
import outputformat.OutputFormat;
import tool.ReduceContext;
import tool.ReduceContextImpl;
import tool.Reducer;
import tool.WrappedReducer;
import configuration.Configuration;
import dfs.DFSClient;
import fileSplit.RemoteSplitOperator;

public class ReduceTask implements Task {
	/**
	 * 
	 */
	private static final long serialVersionUID = 9078394524555201820L;
	private ArrayList<MapOutput> inputData;
	private JobConf jobConf;
	private TaskAttemptID taskAttemptID;
	private String outputPath;
	private Configuration conf;
	private String localPath;

	public ReduceTask(ArrayList<MapOutput> list, JobConf jobConf,
			TaskAttemptID taskAttemptID) {
		this.inputData = list;
		this.taskAttemptID = taskAttemptID;
		this.jobConf = jobConf;
		this.conf = new Configuration();
	}

	private String generateInputPath() {
		String path = localPath + conf.reduceShufflePath
				+ getTaskAttemptID().toString();
		System.out.println("shuffle path: " + path);
		File dir = new File(path);
		if (!dir.exists()) {
			if (!dir.mkdirs()) {
				throw new RuntimeErrorException(null,
						"make reduce shuffle path dir error!");
			}
		}
		return path;
	}

	private String generateOutputPath() {
		outputPath = localPath + conf.reduceInterPath
				+ taskAttemptID.toString();
		System.out.println("output path: " + outputPath);
		File dir = new File(outputPath);
		if (!dir.exists()) {
			if (!dir.mkdirs()) {
				throw new RuntimeErrorException(null,
						"make reduce local output path dir error!");
			}
		}
		return outputPath;
	}

	public void setLocalPath(String localPath) {
		this.localPath = localPath;
	}

	@Override
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void run() throws IOException, InterruptedException,
			ClassNotFoundException, InstantiationException,
			IllegalAccessException {
		ReduceCopier reduceCopier = new ReduceCopier(inputData,
				generateInputPath(), taskAttemptID);
		if (!reduceCopier.fetchOutputs()) {
			throw new RuntimeException("fetch map output files error!");
		}

		Class<?> keyClass = Class.forName(jobConf.getMapOutputKeyClass());
		Class<?> valueClass = Class.forName(jobConf.getMapOutputValueClass());
		runNewReducer(jobConf, keyClass, valueClass,
				reduceCopier.intermediatePath);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private <INKEY, INVALUE, OUTKEY, OUTVALUE> void runNewReducer(
			JobConf jobConf, Class<INKEY> keyClass, Class<INVALUE> valueClass,
			String intermediateOutput) throws IOException,
			InterruptedException, ClassNotFoundException,
			InstantiationException, IllegalAccessException {

		// make a reducer
		Class<?> reduceClass = Class.forName(jobConf.getReducerClass());
		Reducer<INKEY, INVALUE, OUTKEY, OUTVALUE> reducer = (Reducer<INKEY, INVALUE, OUTKEY, OUTVALUE>) reduceClass
				.newInstance();
		// make the output
		Class<?> outputFormatClass = Class.forName(jobConf.getOutputFormat());
		OutputFormat outputFormat = (OutputFormat) outputFormatClass
				.newInstance();
		RecordWriter<OUTKEY, OUTVALUE> output = outputFormat
				.getRecordWriter(getDataOutputStream());
		// make the input
		ReduceRecordReader<INKEY, INVALUE> input = new ReduceRecordReader();
		input.initialize(intermediateOutput);
		ReduceContext<INKEY, INVALUE, OUTKEY, OUTVALUE> reduceContext = new ReduceContextImpl<INKEY, INVALUE, OUTKEY, OUTVALUE>(
				getTaskAttemptID(), input, output);

		Reducer<String, String, OUTKEY, OUTVALUE>.Context reducerContext = new WrappedReducer<String, String, OUTKEY, OUTVALUE>()
				.getReducerContext((ReduceContext<String, String, OUTKEY, OUTVALUE>) reduceContext);
		reducer.run((Reducer<INKEY, INVALUE, OUTKEY, OUTVALUE>.Context) reducerContext);
		output.close();
		DFSClient dfsClient = new DFSClient();
		dfsClient.uploadFile(generateOutputPath(), jobConf.getOutputPath()
				+ taskAttemptID.toString());
	}

	private DataOutputStream getDataOutputStream() throws FileNotFoundException {
		return new DataOutputStream(new FileOutputStream(generateOutputPath()));
	}

	@Override
	public Task setOutputFile(String fileName) {
		return this;
	}

	@Override
	public Task setTaskID(TaskAttemptID id) {
		this.taskAttemptID = id;
		return this;
	}

	@Override
	public String getOutputFile() {
		return jobConf.getOutputPath() + this.taskAttemptID.toString();
	}

	@Override
	public String getInputFile() {
		return null;
	}

	@Override
	public JobConf getJobConf() {
		return jobConf;
	}

	@Override
	public TaskAttemptID getTaskAttemptID() {
		return this.taskAttemptID;
	}

	@Override
	public Task setJobConf(JobConf jobConf) {
		this.jobConf = jobConf;
		return this;
	}

	public ArrayList<MapOutput> getInputData() {
		return inputData;
	}

	public void setInputData(ArrayList<MapOutput> inputData) {
		this.inputData = inputData;
	}

	public static class ReduceCopier<K, V> implements ShuffleConsumerPlugin {

		private ArrayList<MapOutput> inputData;
		private String intermediatePath;
		private TreeMap<String, ValueData> shuffleData = new TreeMap<String, ValueData>();
		private TaskAttemptID taskAttemptID;
		public static final String separator = "\t";

		public ReduceCopier(ArrayList<MapOutput> inputData,
				String intermediateOutput, TaskAttemptID taskAttemptID) {
			this.inputData = inputData;
			this.taskAttemptID = taskAttemptID;
			this.intermediatePath = intermediateOutput + "/"
					+ taskAttemptID.toString();
		}

		@Override
		public boolean fetchOutputs() throws IOException {
			for (MapOutput input : inputData) {
				RemoteSplitOperator remoteSplitOperator = new RemoteSplitOperator();
				System.out.println("fetch output: " + input.getLocalFilePath());
				List<String> data = remoteSplitOperator.readBlock(
						input.getNodeID(), input.getLocalFilePath());
				for (String val : data) {
					String[] keyValuePair = val.split("\t");
					String key = keyValuePair[0];
					String value = keyValuePair[1];
					if (shuffleData.containsKey(key)) {
						shuffleData.get(key).add(value);
					} else {
						ValueData valueData = new ValueData();
						valueData.add(value);
						shuffleData.put(key, valueData);
					}
				}
			}
			DataOutputStream dataoutputStream = new DataOutputStream(
					new FileOutputStream(intermediatePath));
			LineRecordWriter<String, String> lineRecordWriter = new LineRecordWriter<String, String>(
					dataoutputStream);
			for (Entry<String, ValueData> line : shuffleData.entrySet()) {
				String key = line.getKey();
				String value = line.getValue().toString();
				lineRecordWriter.write(key, value);
			}
			lineRecordWriter.close();
			return true;
		}

		public class ValueData {
			ArrayList<String> valueData = new ArrayList<String>();

			public void add(String value) {
				valueData.add(value);

			}

			public ArrayList<String> getValueData() {
				return valueData;
			}

			public void setValueData(ArrayList<String> valueData) {
				this.valueData = valueData;
			}

			public String toString() {
				StringBuilder sb = new StringBuilder();
				for (int i = 0; i < valueData.size(); i++) {
					sb.append(valueData.get(i)).append(separator);
				}
				return sb.toString().substring(0, sb.toString().length() - 1);
			}
		}

	}

	@Override
	public JobID getJobID() {
		return jobConf.getJobID();
	}

	@Override
	public Task setInputFile(String fileName) {
		return null;
	}

	public void addMapOutput(MapOutput mapOutput) {
		inputData.add(mapOutput);
	}

}
