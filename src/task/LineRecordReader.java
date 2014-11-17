package task;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import fileSplit.MapInputSplit;

public class LineRecordReader extends RecordReader<Long, String> {

	private BufferedReader br;
	// key is the offset
	private Long key;
	private String value;

	public LineRecordReader() {
		key = null;
		value = null;
	}

	@Override
	public void initialize(MapInputSplit split, String host)
			throws FileNotFoundException {
		String path = split.getPath();
		// fileName here is the split fileName, not the original fileName
		String fileName = split.getFileName() + "_" + split.getBlockIndex();
		FileReader fr = null;
		fr = new FileReader(path + File.separator + fileName);
		br = new BufferedReader(fr);
	}

	@Override
	public boolean nextKeyValue() throws IOException {
		String line;
		line = br.readLine();
		if (line == null) {
			return false;
		}
		if (key == null) {
			key = 0L;
		} else {
			++key;
		}
		value = line;
		return true;
	}

	@Override
	public Long getCurrentKey() {
		return key;
	}

	@Override
	public String getCurrentValue() {
		return value;
	}

	@Override
	public void close() throws IOException {
		br.close();
	}

}
