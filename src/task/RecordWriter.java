package task;

import java.io.File;
import java.io.FileOutputStream;
import java.util.List;

public class RecordWriter {
	private String fileName;
	private int recordLen;

	public RecordWriter(String fn, int len) {
		fileName = fn;
		recordLen = len;
	}

	public void writeRecord(List<String[]> keyValue) {
		FileOutputStream fos = null;

		try {
			fos = new FileOutputStream(new File(fileName), false);

			String record;
			for (int i = 0; i < keyValue.size(); i++) {
				record = keyValue.get(i)[0].trim() + "\t"
						+ keyValue.get(i)[1].trim() + "\n";
				byte[] bytes = record.getBytes();

				if (bytes.length > recordLen) {
					System.out.println("Record too long to write");
					continue;
				}

				fos.write(record.getBytes());
			}
			fos.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void write(String key, String value) {

	}
}
