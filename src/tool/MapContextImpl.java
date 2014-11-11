package tool;

import java.io.IOException;

import Configuration.MyConfiguration;
import task.RecordReader;
import task.RecordWriter;
import task.TaskAttemptID;

public class MapContextImpl<KEYIN, VALUEIN, KEYOUT, VALUEOUT> implements
		MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

	private RecordReader reader;
	private MyInputSplit split;
	private RecordWriter writer;
	private MyConfiguration conf;

	public MapContextImpl(MyConfiguration conf, TaskAttemptID taskid,
			RecordReader reader, RecordWriter writer, MyInputSplit split) {
		this.reader = reader;
		this.split = split;
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
	public void write(String key, String value) throws IOException,
			InterruptedException {
		writer.write(key, value);
	}

}
