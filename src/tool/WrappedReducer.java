package tool;

import java.io.IOException;

public class WrappedReducer extends Reducer {

	/**
	 * A a wrapped {@link Reducer.Context} for custom implementations.
	 * 
	 * @param reduceContext
	 *            <code>ReduceContext</code> to be wrapped
	 * @return a wrapped <code>Reducer.Context</code> for custom implementations
	 */
	public Reducer.Context getReducerContext(ReduceContext reduceContext) {
		return (Reducer.Context) new Context(reduceContext);
	}

	public class Context extends Reducer.Context {

		protected ReduceContext reduceContext;

		public Context(ReduceContext reduceContext) {
			this.reduceContext = reduceContext;
		}

		@Override
		public boolean nextKey() throws IOException, InterruptedException {
			return reduceContext.nextKey();
		}

		@Override
		public Iterable<String> getValues() throws IOException,
				InterruptedException {
			return reduceContext.getValues();
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			return reduceContext.nextKeyValue();
		}

		@Override
		public String getCurrentKey() throws IOException, InterruptedException {
			return reduceContext.getCurrentKey();
		}

		@Override
		public String getCurrentValue() throws IOException,
				InterruptedException {
			return reduceContext.getCurrentValue();
		}

		@Override
		public void write(String key, String value) throws IOException,
				InterruptedException {
			reduceContext.write(key, value);
		}
	}
}
