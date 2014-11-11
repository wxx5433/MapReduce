package node;

import java.io.Serializable;

public class NodeID implements Comparable<NodeID>, Serializable {
	private static final long serialVersionUID = -4871124155025672340L;
	
	private String ip;
	private int port;
	private String host;
	private int blockCount;
	private String rootPath;
	
	public NodeID(String ip, int port) {
		this.ip = ip;
		this.port = port;
		this.host = this.toString();
		this.blockCount = 0;
	}
	
	public NodeID(String ip, int port, String rootPath) {
		this(ip, port);
		this.rootPath = rootPath;
	}
	
	public String getRootPath() {
		return rootPath;
	}

	public synchronized void incrementBlockCount() {
		this.blockCount++;
	}
	
	public synchronized int getBlockCount() {
		return blockCount;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}
	
	public static NodeID constructFromString(String host) {
		int divideIndex = host.indexOf(":");
		String ip = host.substring(0, divideIndex);
		int port = Integer.parseInt(host.substring(divideIndex + 1));
		return new NodeID(ip, port);
	}

	@Override
	public int hashCode() {
		return host.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (obj instanceof NodeID) {
			NodeID that = (NodeID)obj;
			if (this.ip.equals(that.ip) && this.port == that.port) {
				return true;
			}
		}
		return false;
	}

	@Override
	public String toString() {
		return ip + ":" + Integer.toString(port);
	}

	@Override
	public int compareTo(NodeID o) {
		return this.blockCount - o.blockCount;
	}
	
	
	
}
