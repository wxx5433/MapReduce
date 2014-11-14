package tool;

import configuration.MyConfiguration;


public class MyToolRunner {

	public static int run(MyConfiguration conf, MyTool tool, String[] args)
			throws Exception {
		if (conf == null) {
			conf = new MyConfiguration();
		}
		tool.setConf(conf);
		return tool.run(args);
	}
	
	public static int run(MyTool tool, String[] args) throws Exception {
		return run(tool.getConf(), tool, args);
	}
}
