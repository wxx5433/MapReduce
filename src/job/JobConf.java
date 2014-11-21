package job;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import configuration.ConfigurationStrings;

public class JobConf implements Serializable {

	private static final long serialVersionUID = 8799187146592451090L;

	private String jobName;
	private JobID jobId;

	private int reduceNum;

	private String inputPath;
	private String outputPath;

	private static final Map<String, String> classMap = new HashMap<String, String>();

	public void setJobId(JobID jobId) {
		this.jobId = jobId;
	}

	public JobID getJobID() {
		return this.jobId;
	}

	public void setJobName(String name) {
		this.jobName = name;
	}

	public String getJobName() {
		return jobName;
	}

	public String getInputPath() {
		return inputPath;
	}

	public void setInputPath(String inputPath) {
		this.inputPath = inputPath;
	}

	public String getOutputPath() {
		return outputPath;
	}

	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}

	public String getInputFormat() {
		return get(ConfigurationStrings.INPUTFORMAT_CLASS);
	}

	public void setInputFormat(Class<?> theClass) {
		setClass(ConfigurationStrings.INPUTFORMAT_CLASS, theClass);
	}

	public String getOutputFormat() {
		return get(ConfigurationStrings.OUTPUTFORMAT_CLASS);
	}

	public void setOutputFormat(Class<?> theClass) {
		setClass(ConfigurationStrings.OUTPUTFORMAT_CLASS, theClass);
	}

	public void setNumReduceTasks(int tasks) {
		reduceNum = tasks;
	}

	public int getNumReduceTasks() {
		return reduceNum;
	}

	public void setMapperClass(Class<?> theClass) {
		setClass(ConfigurationStrings.MAPPER_CLASS, theClass);
	}

	public void setReducerClass(Class<?> theClass) {
		setClass(ConfigurationStrings.REDUCER_CLASS, theClass);
	}

	public String getMapperClass() {
		return get(ConfigurationStrings.MAPPER_CLASS);
	}

	public String getReducerClass() {
		return get(ConfigurationStrings.REDUCER_CLASS);
	}

	public String getPartitionerClass() {
		return get(ConfigurationStrings.PARTITIONER_CLASS);
	}

	public void setPartionerClass(Class<?> theClass) {
		setClass(ConfigurationStrings.PARTITIONER_CLASS, theClass);
	}

	public String getMapOutputKeyClass() {
		return get(ConfigurationStrings.MAP_OUTPUT_KEY_CLASS);
	}

	public void setMapOutputKeyClass(Class<?> theClass) {
		setClass(ConfigurationStrings.MAP_OUTPUT_KEY_CLASS, theClass);
	}

	public String getMapOutputValueClass() {
		return get(ConfigurationStrings.MAP_OUTPUT_VALUE_CLASS);
	}

	public void setMapOutputValueClass(Class<?> theClass) {
		setClass(ConfigurationStrings.MAP_OUTPUT_VALUE_CLASS, theClass);
	}

	public void setClass(String name, Class<?> theClass) {
		set(name, theClass.getName());
	}

	private void set(String name, String className) {
		classMap.put(name, className);
	}

	private String get(String name) {
		if (classMap.get(name) != null) {
			return classMap.get(name);
		} else
			return null;
	}

}
