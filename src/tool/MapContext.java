package tool;

import java.io.IOException;

public interface MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
	/**
	 * Advance to the next key, value pair, returning null if at end.
	 * 
	 * @return the key object that was read into, or null if no more
	 */
	public boolean nextKeyValue() throws IOException, InterruptedException;

	/**
	 * Get the current key.
	 * 
	 * @return the current key object or null if there isn't one
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public String getCurrentKey() throws IOException, InterruptedException;

	/**
	 * Get the current value.
	 * 
	 * @return the value object that was read into
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public String getCurrentValue() throws IOException, InterruptedException;

	/**
	 * Generate an output key/value pair.
	 */
	public void write(String key, String value) throws IOException,
			InterruptedException;
}
