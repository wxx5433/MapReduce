package tool;

import java.io.IOException;

public class WrappedReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends
		Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

	/**
	 * A a wrapped {@link Reducer.Context} for custom implementations.
	 * 
	 * @param reduceContext
	 *            <code>ReduceContext</code> to be wrapped
	 * @return a wrapped <code>Reducer.Context</code> for custom implementations
	 */
	public Reducer<String, String, KEYOUT, VALUEOUT>.Context getReducerContext(
			ReduceContext<String, String, KEYOUT, VALUEOUT> reduceContext) {
		return (Reducer<String, String, KEYOUT, VALUEOUT>.Context) new Context(
				reduceContext);
	}

	public class Context extends
			Reducer<String, String, KEYOUT, VALUEOUT>.Context {

		protected ReduceContext<String, String, KEYOUT, VALUEOUT> reduceContext;

		public Context(
				ReduceContext<String, String, KEYOUT, VALUEOUT> reduceContext) {
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
		public void write(KEYOUT key, VALUEOUT value) throws IOException,
				InterruptedException {
			reduceContext.write(key, value);
		}
	}
}
