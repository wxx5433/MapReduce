package inputformat;

import java.io.IOException;

import job.JobConf;
import task.RecordReader;
import fileSplit.InputSplit;

public interface InputFormat<K, V> {
	RecordReader<K, V> getRecordReader(InputSplit split, JobConf job)
			throws IOException;
}
