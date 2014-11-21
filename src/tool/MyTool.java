package tool;

public interface MyTool {
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
