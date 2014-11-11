package tool;

import java.io.IOException;
import java.util.List;

import task.RecordReader;
import task.RecordWriter;
import task.TaskAttemptID;
import configuration.MyConfiguration;

public class ReduceContextImpl<KEYIN, VALUEIN, KEYOUT, VALUEOUT> implements
		ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

	private RecordReader reader;
	private List<MyInputSplit> splitList;
	private RecordWriter writer;
	private MyConfiguration conf;

	public ReduceContextImpl(MyConfiguration conf, TaskAttemptID taskid,
			RecordReader reader, RecordWriter writer,
			List<MyInputSplit> splitList) {
		this.reader = reader;
		this.splitList = splitList;
		this.writer = writer;
		this.conf = conf;
	}

	@Override
	public boolean nextKey() throws IOException, InterruptedException {
		return reader.nextKey();
	}

	@Override
	public Iterable<String> getValues() throws IOException,
			InterruptedException {
		return reader.getValues();
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
		return reader.getCurrentKey();
	}

	@Override
	public void write(String key, String value) throws IOException,
			InterruptedException {
		writer.write(key, value);
	}

}
