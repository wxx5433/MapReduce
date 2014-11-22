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
			return mapContext.nextKeyValue();
		}

		@Override
		public KEYIN getCurrentKey() throws IOException, InterruptedException {
			return mapContext.getCurrentKey();
		}

		@Override
		public VALUEIN getCurrentValue() throws IOException,
				InterruptedException {
			return mapContext.getCurrentValue();
		}

		@Override
		public void write(KEYOUT key, VALUEOUT value) throws IOException,
				InterruptedException {
			mapContext.write(key, value);
		}

	}
}
