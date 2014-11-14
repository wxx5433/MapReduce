package job;

import configuration.MyConfiguration;


public class JobConf {
	
	private String jobName;

	private int reduceNum;

	private String inputPath;
	private String outputPath;
	
	private String inputFormat;
	private String outputFormat;
	
	private Class mapperClass;
	private Class reducerClass;

	public JobConf(MyConfiguration conf) {
		// TODO Auto-generated constructor stub
	}

	public void setJobName(String name) {
		this.jobName = name;
	}
	
	public String getJobName() {
		return jobName;
	}

	public int getReduceNum() {
		return reduceNum;
	}

	public void setReduceNum(int reduceNum) {
		this.reduceNum = reduceNum;
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

	public void setNumReduceTasks(int tasks) {
		// TODO Auto-generated method stub

	}

	public int getNumReduceTasks() {
		// TODO Auto-generated method stub
		return 0;
	}

	public boolean getUseNewMapper() {
		// TODO Auto-generated method stub
		return false;
	}


	public void setMapperClass(String name, Class<?> theClass) {

	}

	public void setReducerClass(String name, Class<?> theClass) {

	}

	public String getMapperClass(String name) {
		return null;

	}

	public String getReducerClass(String name) {
		return null;
	}

}
