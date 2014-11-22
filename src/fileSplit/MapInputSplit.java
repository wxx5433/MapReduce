package fileSplit;

import java.io.Serializable;

import node.NodeID;

public class MapInputSplit extends InputSplit implements Serializable {
	private static final long serialVersionUID = 6600132293414194730L;
	
	private String fileName;
	private int blockIndex;
	private String host;
	private String path;
	private NodeID dataNodeID;

	public MapInputSplit(String fileName, int blockIndex) {
		this.fileName = fileName;
		this.blockIndex = blockIndex;
	}
	
	public MapInputSplit(String fileName, int blockIndex, String host) {
		this(fileName, blockIndex);
		this.host = host;
	}
	
	public MapInputSplit(String fileName, int blockIndex, String host, String path) {
		this(fileName, blockIndex, host);
		this.path = path;
	}
	
	public MapInputSplit(FileSplit split) {
		this.fileName = split.getFileName();
		this.blockIndex = split.getBlockIndex();
		dataNodeID = split.getOneDataNode();
		this.host = dataNodeID.toString();
		this.path = split.getPath(this.host);
	}
	
	@Override
	public NodeID getOneDataNode() {
		return NodeID.constructFromString(host);
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public int getBlockIndex() {
		return blockIndex;
	}

	public void setBlockIndex(int blockIndex) {
		this.blockIndex = blockIndex;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public NodeID getDataNodeID() {
		return dataNodeID;
	}

	public void setDataNodeID(NodeID dataNodeID) {
		this.dataNodeID = dataNodeID;
	}
	
}
