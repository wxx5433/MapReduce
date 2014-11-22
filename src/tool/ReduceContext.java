package tool;

import java.io.IOException;
import java.util.Iterator;

public interface ReduceContext {
	/** Start processing next unique key. */
	public boolean nextKey() throws IOException, InterruptedException;

	/**
	 * Iterate through the values for the current key, reusing the same value
	 * object, which is stored in the context.
	 * 
	 * @return the series of values associated with the current key. All of the
	 *         objects returned directly and indirectly from this method are
	 *         reused.
	 */
	public Iterable<String> getValues() throws IOException,
			InterruptedException;

	/**
	 * {@link Iterator} to iterate over values for a given group of records.
	 */
	interface ValueIterator<String> extends Iterator<String> {

	}

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
