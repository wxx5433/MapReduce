package tool;

import java.io.IOException;

public class Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

	/**
	 * The <code>Context</code> passed on to the {@link Mapper} implementations.
	 */
	public abstract class Context implements
			MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
	}

	/**
	 * Called once for each key/value pair in the input split. Most applications
	 * should override this, but the default is the identity function.
	 */
	@SuppressWarnings("unchecked")
	protected void map(KEYIN key, VALUEIN value, Context context)
			throws IOException, InterruptedException {
		context.write((KEYOUT) key, (VALUEOUT) value);
	}

	/**
	 * Expert users can override this method for more complete control over the
	 * execution of the Mapper.
	 * 
	 * @param context
	 * @throws IOException
	 */
	public void run(Context context) throws IOException, InterruptedException {
		while (context.nextKeyValue()) {
			System.out.println("key: " + context.getCurrentKey());
			System.out.println("Value:" + context.getCurrentValue());
			map(context.getCurrentKey(), context.getCurrentValue(), context);
		}
	}
}
