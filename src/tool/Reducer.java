package tool;

import java.io.IOException;

public class Reducer {

	/**
	 * The <code>Context</code> passed on to the {@link Reducer}
	 * implementations.
	 */
	public abstract class Context implements ReduceContext {
	}

	/**
	 * This method is called once for each key. Most applications will define
	 * their reduce class by overriding this method. The default implementation
	 * is an identity function.
	 */
	@SuppressWarnings("unchecked")
	protected void reduce(String key, Iterable<String> values, Context context)
			throws IOException, InterruptedException {
		for (String value : values) {
			context.write((String) key, (String) value);
		}
	}

	/**
	 * Advanced application writers can use the
	 * {@link #run(org.apache.hadoop.mapreduce.Reducer.Context)} method to
	 * control how the reduce task works.
	 */
	public void run(Context context) throws IOException, InterruptedException {
		while (context.nextKey()) {
			reduce(context.getCurrentKey(), context.getValues(), context);
		}
	}
}