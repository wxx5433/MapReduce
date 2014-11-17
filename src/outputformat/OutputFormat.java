package outputformat;

import java.io.DataOutputStream;
import java.io.IOException;

import task.RecordWriter;

public interface OutputFormat<K, V> {
	RecordWriter<K, V> getRecordWriter(DataOutputStream dataOutputStream)
			throws IOException;
}
