package inputformat;

import java.io.IOException;

import job.JobConf;
import task.LineRecordReader;
import task.RecordReader;
import fileSplit.MapInputSplit;

public class LineInputFormat<KEY, VALUE> implements InputFormat<KEY, VALUE> {

	@Override
	public RecordReader<KEY, VALUE> getRecordReader(MapInputSplit split,
			JobConf job) throws IOException {
		RecordReader recordReader = new LineRecordReader();
		recordReader.initialize(split, split.getHost());
		return recordReader;
	}

}
