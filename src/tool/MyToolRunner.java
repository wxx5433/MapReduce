package tool;

import configuration.Configuration;

public class MyToolRunner {

	public static int run(Configuration conf, MyTool tool, String[] args)
			throws Exception {
		if (conf == null) {
			conf = new Configuration();
		}
		return tool.run(args);
	}

	public static int run(MyTool tool, String[] args) throws Exception {
		return run(tool, args);
	}
}
