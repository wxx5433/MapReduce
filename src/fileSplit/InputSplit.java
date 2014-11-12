package fileSplit;

import node.NodeID;

public abstract class InputSplit {
	/**
	 * get one dataNode who store the file split
	 * @return
	 */
	public abstract NodeID getOneDataNode();
}
