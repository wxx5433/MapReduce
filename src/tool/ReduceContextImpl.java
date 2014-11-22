package tool;

import java.io.IOException;

import task.RecordWriter;
import task.ReduceRecordReader;
import task.TaskAttemptID;
import fileSplit.MapInputSplit;

public class ReduceContextImpl<KEYIN, VALUEIN, KEYOUT, VALUEOUT> implements
		ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

	private RecordWriter<KEYOUT, VALUEOUT> writer;
	private ReduceRecordReader<KEYIN, VALUEIN> reader;

	public ReduceContextImpl(TaskAttemptID taskid,
			ReduceRecordReader<KEYIN, VALUEIN> reader,
			RecordWriter<KEYOUT, VALUEOUT> writer) {
		this.reader = reader;
		this.writer = writer;
	}

	public ReduceContextImpl(TaskAttemptID taskid,
			ReduceRecordReader<KEYIN, VALUEIN> reader,
			RecordWriter<KEYOUT, VALUEOUT> writer, MapInputSplit split) {
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
	public void write(KEYOUT key, VALUEOUT value) throws IOException,
			InterruptedException {
		writer.write(key, value);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return false;
	}

}
