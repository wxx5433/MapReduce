package outputformat;

import java.io.DataOutputStream;
import java.io.IOException;

import task.LineRecordWriter;
import task.RecordWriter;

public class LineOutputFormat<KEY, VALUE> implements OutputFormat<KEY, VALUE> {

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public RecordWriter<KEY, VALUE> getRecordWriter(
			DataOutputStream dataOutputStream) throws IOException {
		return new LineRecordWriter(dataOutputStream);
	}

}
