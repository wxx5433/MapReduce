package inputformat;

import java.io.IOException;

import job.JobConf;
import task.RecordReader;
import fileSplit.MapInputSplit;

public interface InputFormat<K, V> {
	RecordReader<K, V> getRecordReader(MapInputSplit split, JobConf job)
			throws IOException;
}
