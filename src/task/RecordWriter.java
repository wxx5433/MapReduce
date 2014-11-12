package task;

import java.io.IOException;

public abstract class RecordWriter<KEYIN, VALUEIN> {

	/**
	 * Write a key-value pair
	 * @param key
	 * @param value
	 * @throws IOException 
	 */
	public abstract void write(KEYIN key, VALUEIN value) throws IOException;
	
	/**
	 * close the output stream
	 * @throws IOException 
	 */
	public abstract void close() throws IOException;
		
}
