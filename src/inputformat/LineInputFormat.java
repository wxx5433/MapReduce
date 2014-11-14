package inputformat;

import java.io.IOException;

import job.JobConf;
import task.LineRecordReader;
import task.RecordReader;
import fileSplit.InputSplit;

public class LineInputFormat<Long, String> implements InputFormat<Long, String> {

	@Override
	public RecordReader<Long, String> getRecordReader(InputSplit split,
			JobConf job) throws IOException {
		RecordReader recordReader = new LineRecordReader();
		recordReader.initialize(split, host);
		return;
	}

}
