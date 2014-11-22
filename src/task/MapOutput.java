package task;

import java.io.Serializable;

import node.NodeID;

public class MapOutput implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 6839957624053258682L;
	private NodeID nodeID;
	private String localFilePath;

	public MapOutput(NodeID nodeID, String localFilePath) {
		this.nodeID = nodeID;
		this.localFilePath = localFilePath;
	}

	public NodeID getNodeID() {
		return nodeID;
	}

	public void setNodeID(NodeID nodeID) {
		this.nodeID = nodeID;
	}

	public String getLocalFilePath() {
		return localFilePath;
	}

	public void setLocalFilePath(String localFilePath) {
		this.localFilePath = localFilePath;
	}

}
