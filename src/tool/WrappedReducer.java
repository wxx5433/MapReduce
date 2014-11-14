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
	public Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context getReducerContext(
			ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> reduceContext) {
		return new Context(reduceContext);
	}

	public class Context extends
			Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context {

		protected ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> reduceContext;

		public Context(
				ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> reduceContext) {
			this.reduceContext = reduceContext;
		}

		@Override
		public boolean nextKey() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public Iterable<String> getValues() throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public String getCurrentKey() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getCurrentValue() throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void write(String key, String value) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub

		}
	}
}
