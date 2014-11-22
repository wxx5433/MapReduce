package task;

import java.io.IOException;
import java.util.List;

import node.NodeID;
import configuration.Configuration;
import fileSplit.MapInputSplit;
import fileSplit.RemoteSplitOperator;

public class LineRecordReader extends RecordReader<Long, String> {

	private List<String> br;
	// key is the offset
	private Long key;
	private String value;
	private NodeID nodeID;
	private RemoteSplitOperator remoteSplit;
	private int lineCount;
	private int currentLine;

	public LineRecordReader() {
		key = null;
		value = null;
	}

	@Override
	public void initialize(MapInputSplit split, String host) throws IOException {
		nodeID = new NodeID(split.getHost(), new Configuration().dataNodePort);
		// String path = split.getPath();
		// fileName here is the split fileName, not the original fileName
		String fileName = split.getFileName() + "_" + split.getBlockIndex();
		remoteSplit = new RemoteSplitOperator();
		br = remoteSplit.readBlock(nodeID, fileName);
		lineCount = br.size();
		currentLine = 0;
	}

	@Override
	public boolean nextKeyValue() throws IOException {
		if (currentLine == lineCount) {
			return false;
		}
		if (key == null) {
			key = (long) currentLine;
		} else {
			++key;
		}
		value = br.get(currentLine);
		currentLine++;
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
	}

}
