package tool;

import java.io.IOException;

import task.RecordWriter;
import task.ReduceRecordReader;
import task.TaskAttemptID;
import configuration.MyConfiguration;
import fileSplit.MapInputSplit;

public class ReduceContextImpl<KEYIN, VALUEIN, KEYOUT, VALUEOUT> implements
		ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

	private RecordWriter<KEYOUT, VALUEOUT> writer;
	private MyConfiguration conf;
	private ReduceRecordReader<KEYIN, VALUEIN> reader;

	public ReduceContextImpl(MyConfiguration conf, TaskAttemptID taskid,
			ReduceRecordReader<KEYIN, VALUEIN> reader,
			RecordWriter<KEYOUT, VALUEOUT> writer) {
		this.reader = reader;
		this.writer = writer;
		this.conf = conf;
	}

	public ReduceContextImpl(MyConfiguration conf, TaskAttemptID taskid,
			ReduceRecordReader<KEYIN, VALUEIN> reader,
			RecordWriter<KEYOUT, VALUEOUT> writer, MapInputSplit split) {
		this.reader = reader;
		this.writer = writer;
		this.conf = conf;
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		return reader.nextKeyValue();
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
		return false;
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

}
