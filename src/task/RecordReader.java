package task;

import java.io.FileNotFoundException;
import java.io.IOException;

import fileSplit.MapInputSplit;

public abstract class RecordReader<KEYIN, VALUEIN> {

	public abstract void initialize(MapInputSplit split, String host)
			throws FileNotFoundException;

	public abstract boolean nextKeyValue() throws IOException;

	public abstract KEYIN getCurrentKey();

	public abstract VALUEIN getCurrentValue();

	public abstract void close() throws IOException;
}
