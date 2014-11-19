package tasktracker;

import node.NodeID;

public class TaskTracker implements TaskTrackerInterface {
	private NodeID nodeId;
	
	/**
	 * JobInProgress need node information to allocate tasks to 
	 * dataNode contains the file split.
	 * @return
	 */
	public NodeID getNodeId() {
		return this.nodeId;
	}
}
