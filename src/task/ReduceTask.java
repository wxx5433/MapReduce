package task;

import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import job.JobConf;
import job.JobID;
import outputformat.OutputFormat;
import tool.ReduceContext;
import tool.ReduceContextImpl;
import tool.Reducer;
import tool.WrappedReducer;
import configuration.ConfigurationStrings;
import configuration.MyConfiguration;

public class ReduceTask implements Task {
	private ArrayList<MapOutput> inputData;
	private JobConf jobConf;
	private TaskAttemptID taskAttemptID;
	private String outputPath;

	public ReduceTask() {
	}

	private String generatePath() {
		return ConfigurationStrings.REDUCE_INPUT_PATH + getTaskID().toString();
	}

	@Override
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void run(JobConf jobConf) throws IOException, InterruptedException,
			ClassNotFoundException, InstantiationException,
			IllegalAccessException {
		ReduceCopier reduceCopier = new ReduceCopier(inputData, generatePath());
		if (!reduceCopier.fetchOutputs()) {
			throw new RuntimeException("fetch map output files error!");
		}

		if (!reduceCopier.shuffle()) {
			throw new RuntimeException("shuffle error!");
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
				MyConfiguration.getInstance(), getTaskID(), input, output);

		Reducer<String, String, OUTKEY, OUTVALUE>.Context reducerContext = new WrappedReducer<String, String, OUTKEY, OUTVALUE>()
				.getReducerContext((ReduceContext<String, String, OUTKEY, OUTVALUE>) reduceContext);
		reducer.run((Reducer<INKEY, INVALUE, OUTKEY, OUTVALUE>.Context) reducerContext);
		output.close();
	}

	private DataOutputStream getDataOutputStream() throws FileNotFoundException {
		return new DataOutputStream(new FileOutputStream(generatePath()));
	}

	public Task setInputFile(String fileName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Task setOutputFile(String fileName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Task setTaskID(TaskAttemptID id) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getOutputFile() {
		// TODO Auto-generated method stub
		return jobConf.getOutputPath() + this.taskAttemptID.toString();
	}

	@Override
	public String getInputFile() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public JobConf getJobConf() {
		return jobConf;
	}

	@Override
	public TaskAttemptID getTaskID() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Task setJobConf(JobConf jobConf) {
		// TODO Auto-generated method stub
		return null;
	}

	public static class ReduceCopier<K, V> implements ShuffleConsumerPlugin {

		private ArrayList<MapOutput> inputData;
		private String intermediatePath;
		private HashMap<String, ValueData> shuffleData = new HashMap<String, ValueData>();

		public ReduceCopier(ArrayList<MapOutput> inputData,
				String intermediateOutput) {
			this.inputData = inputData;
			this.intermediatePath = intermediateOutput;
		}

		@Override
		public boolean fetchOutputs() throws IOException {
			for (MapOutput input : inputData) {

			}
			return false;
		}

		@Override
		public void close() {

		}

		@Override
		public boolean shuffle() {

			return false;
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
		}

	}

	@Override
	public JobID getJobID() {
		// TODO Auto-generated method stub
		return null;
	}
}
