package tool;

import configuration.MyConfigurable;

public interface MyTool extends MyConfigurable {
	/**
	 * Execute the command with the given arguments.
	 * 
	 * @param args
	 *            command specific arguments.
	 * @return exit code.
	 * @throws Exception
	 */
	int run(String[] args) throws Exception;
}
