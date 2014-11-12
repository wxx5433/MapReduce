package task;

import java.io.DataOutputStream;
import java.io.IOException;

public class LineRecordWriter<K, V> extends RecordWriter<K, V> {

	private DataOutputStream dos;
	private byte[] separator;
	private byte[] newLine;
	
	public LineRecordWriter(DataOutputStream dos) {
		this.dos = dos;
		this.separator = "\t".getBytes();
		this.newLine = "\n".getBytes();
	}
	
	public LineRecordWriter(DataOutputStream dos, String separator) {
		this.dos = dos;
		this.separator = separator.getBytes();
		this.newLine = "\n".getBytes();
	}
	
	private void writeObject(Object o) throws IOException {
		if (o instanceof String) {
			dos.write(((String) o).getBytes());
		} else {
			dos.write(o.toString().getBytes());
		}
	}
	
	@Override
	public void close() throws IOException {
		dos.close();
	}

	@Override
	public void write(K key, V value) throws IOException {
		if (key == null && value == null) {
			return;
		}
		if (key != null) {
			writeObject(key);
		}
		if (value != null) {
			dos.write(separator);
			writeObject(value);
		}
		dos.write(newLine);
	}

}
