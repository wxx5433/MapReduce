package tool;

import java.io.IOException;

import task.RecordWriter;
import task.ReduceRecordReader;
import task.TaskAttemptID;
import fileSplit.MapInputSplit;

public class ReduceContextImpl implements ReduceContext {

	private RecordWriter<String, String> writer;
	private ReduceRecordReader<String, String> reader;

	public ReduceContextImpl(TaskAttemptID taskid,
			ReduceRecordReader<String, String> reader,
			RecordWriter<String, String> writer) {
		this.reader = reader;
		this.writer = writer;
	}

	public ReduceContextImpl(TaskAttemptID taskid,
			ReduceRecordReader<String, String> reader,
			RecordWriter<String, String> writer, MapInputSplit split) {
		this.reader = reader;
		this.writer = writer;
	}

	@Override
	public String getCurrentKey() throws IOException, InterruptedException {
		return reader.getCurrentKey();
	}

	@Override
	public String getCurrentValue() throws IOException, InterruptedException {
		return reader.getCurrentValue();
	}

	@Override
	public boolean nextKey() throws IOException, InterruptedException {
		return reader.nextKey();
	}

	@Override
	public Iterable<String> getValues() throws IOException,
			InterruptedException {
		return reader.getCurrentValues();
	}

	@Override
	public void write(String key, String value) throws IOException,
			InterruptedException {
		writer.write(key, value);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return false;
	}

}
