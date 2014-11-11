package job;

import configuration.MyConfiguration;

public class JobConf {

	public JobConf(MyConfiguration conf) {
		// TODO Auto-generated constructor stub
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

	public void setJobName(String name) {
		// TODO Auto-generated method stub

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
