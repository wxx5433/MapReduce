package tool;

import java.io.IOException;

public class WrappedMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends
		Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

	/**
	 * Get a wrapped {@link Mapper.Context} for custom implementations.
	 * 
	 * @param mapContext
	 *            <code>MapContext</code> to be wrapped
	 * @return a wrapped <code>Mapper.Context</code> for custom implementations
	 */
	public Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context getMapContext(
			MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> mapContext) {
		return new Context(mapContext);
	}

	public class Context extends
			Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context {

		protected MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> mapContext;

		public Context(MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> mapContext) {
			this.mapContext = mapContext;
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
