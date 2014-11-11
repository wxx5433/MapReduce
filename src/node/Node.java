package node;

/**
 * Just put the hostname/IP address and port number all in And override some
 * functions to make this class can be the key of map
 * 
 * @author Xiaoxiang Wu(xiaoxiaw)
 * @author Ye Zhou(yezhou)
 */
public class Node {
	private String hostName;
	private int port;
	private final String nodeID;
	private final int hash;

	public Node(String hostName, int port) {
		this.hostName = hostName;
		this.port = port;
		nodeID = hostName + ":" + port;
		hash = nodeID.hashCode();
	}

	public String getHostName() {
		return hostName;
	}

	public int getPort() {
		return port;
	}

	@Override
	public int hashCode() {
		return hash;
	}

	@Override
	public String toString() {
		return nodeID;
	}

	public static Node fromString(String nodeIdStr) {
		int divideIndex = nodeIdStr.indexOf(":");
		String host = nodeIdStr.substring(0, divideIndex);
		int port = Integer.parseInt(nodeIdStr.substring(divideIndex + 1));
		return new Node(host, port);
	}

	@Override
	public boolean equals(Object anObject) {
		if (this == anObject) {
			return true;
		}
		if (anObject instanceof Node) {
			Node anotherNodeID = (Node) anObject;
			if (port == anotherNodeID.port
					&& hostName.equals(anotherNodeID.hostName))
				return true;
		}
		return false;
	}

}
