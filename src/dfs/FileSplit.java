package dfs;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import configuration.Configuration;

/**
 * Every file uploaded to DFS will be split into several parts. 
 * In our system, the file is split according to line number, which is specified
 * in the configuration file. 
 * In this way, we can avoid the potentiality to split one line into two
 * different parts, which is a problem if using file size to do the split. 
 * However, there may be vary length of lines, which will make the split skewed in size.
 * @author Xiaoxiang Wu (xiaoxiaw)
 * @author Ye Zhou (yezhou)
 */
public class FileSplit implements Serializable, Comparable<FileSplit> {


	private static final long serialVersionUID = -8773858158603105299L;

	private String fileName;
	private int blockIndex;
	// key: hosts(ip:port), value: localPath
	private Map<String, String> paths;
	private int hostsCount;

	public FileSplit(String fileName, int blockIndex) {
		this.fileName = fileName;
		this.blockIndex = blockIndex;
		this.paths = new ConcurrentHashMap<String, String>();
		this.hostsCount = 0;
	}
	
	public synchronized void addHost(String host, String path) throws Exception {
		if (hostsCount >= Configuration.replicaNum) {
			throw new Exception("There are too many replica for the file split!");
		}
		paths.put(host, path);
		++hostsCount;
	}

	public synchronized void removeHost(String host) {
		if (!paths.containsKey(host)) {
			try {
				throw new Exception("The host has no replica for this split!");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		paths.remove(host);
		--hostsCount;
	}

	@Override
	public int hashCode() {
		return (fileName + "_" + blockIndex).hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (obj instanceof FileSplit) {
			FileSplit that = (FileSplit) obj;
			if (this.fileName.equals(that.fileName) && this.blockIndex == that.blockIndex) {
				return true;
			}
		}
		return false;
	}

	@Override
	public int compareTo(FileSplit o) {
		if (!this.fileName.equals(o.getFileName())) {
			return this.fileName.compareTo(o.getFileName());
		} 
		return Integer.compare(this.blockIndex, o.getBlockIndex());
	}
	
	public List<String> getHosts() {
		return new ArrayList<String>(paths.keySet());
	}
	
	public NodeID getOneHost() {
		List<String> hosts = getHosts();
		if (hosts.size() == 0) {
			return null;
		}
		String host = getHosts().get(0);
		return NodeID.constructFromString(host);
	}
	
	public String getPath(String host) {
		return paths.get(host);
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
}
