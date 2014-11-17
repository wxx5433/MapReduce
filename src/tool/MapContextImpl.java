package tool;

import java.io.IOException;

import task.RecordReader;
import task.RecordWriter;
import task.TaskAttemptID;
import configuration.MyConfiguration;
import fileSplit.MapInputSplit;

public class MapContextImpl<KEYIN, VALUEIN, KEYOUT, VALUEOUT> implements
		MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

	private RecordReader<KEYIN, VALUEIN> reader;
	private MapInputSplit split;
	private RecordWriter<KEYOUT, VALUEOUT> writer;
	private MyConfiguration conf;

	public MapContextImpl(MyConfiguration conf, TaskAttemptID taskid,
			RecordReader<KEYIN, VALUEIN> reader,
			RecordWriter<KEYOUT, VALUEOUT> writer, MapInputSplit split) {
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
	public KEYIN getCurrentKey() throws IOException, InterruptedException {
		return reader.getCurrentKey();
	}

	@Override
	public VALUEIN getCurrentValue() throws IOException, InterruptedException {
		return reader.getCurrentValue();
	}

	@Override
	public void write(KEYOUT key, VALUEOUT value) throws IOException,
			InterruptedException {
		writer.write(key, value);
	}

}
