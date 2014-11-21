package nameNode;

import java.io.File;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;

import node.NodeID;
import configuration.Configuration;
import fileSplit.FileSplit;

public class NameNodeServiceImpl implements NameNodeService {

	private Queue<NodeID> nodes;
	//	private Map<String, Set<FileSplit>> files;
	// String: fileName, Integer: blockIndex.
	private Map<String, Map<Integer, FileSplit>> nameSpace;
	private Configuration configuration;

	public NameNodeServiceImpl(Configuration configuration) throws RemoteException {
		super();
		this.configuration = configuration;
		this.nodes = new PriorityBlockingQueue<NodeID>();
		//		this.files = new ConcurrentHashMap<String, Set<FileSplit>>();
		this.nameSpace = new ConcurrentHashMap<String, Map<Integer, FileSplit>>();
	}

	/**
	 * add a dataNode to management
	 */
	@Override
	public void registerDataNode(String ip, int port, String rootPath) {
		nodes.offer(new NodeID(ip, port, rootPath));
		System.out.println("New dataNode registered!!:" + ip + port);
	}

	@Override
	public Iterable<NodeID> getDataNodesToUpload(String fileName, int blockIndex) {
		List<NodeID> dataNodeIDs = new ArrayList<NodeID>();
		// in case of uploading the same file split
		synchronized (this) {
			if (nameSpace.containsKey(fileName) && nameSpace.get(fileName).containsKey(blockIndex)) {
				return dataNodeIDs;
			}
		}
		FileSplit fileSplit = new FileSplit(fileName, blockIndex);
		int count = 0;
		while (count < configuration.replicaNum && !nodes.isEmpty()) {
			dataNodeIDs.add(nodes.poll());
			++count;
		}
		for (NodeID nodeID: dataNodeIDs) {
			try {
				fileSplit.addHost(nodeID.toString(), nodeID.getRootPath() 
						+ File.separator + fileName + "_" + blockIndex);
			} catch (Exception e) {
				e.printStackTrace();
			}
			nodeID.incrementBlockCount();
			nodes.offer(nodeID);
		}
		// Actually, we need to get response from dataNodes to update
		updateNameSpace(fileName, blockIndex, fileSplit);
		System.out.println("Succesfully get dataNode info from NameNode!");
		return dataNodeIDs;
	}
	
	/**
	 * The method is invoked by DFS client to download a file to client machine.
	 * @param fileName
	 * @return
	 */
	public Iterable<FileSplit> getDataNodesToDownload(String fileName) {
		Queue<FileSplit> result = new PriorityQueue<FileSplit>();
		// cannot find the file
		if (!nameSpace.containsKey(fileName)) {
			return result;
		}
		Map<Integer, FileSplit> splits = nameSpace.get(fileName);
		for (Map.Entry<Integer, FileSplit> split: splits.entrySet()) {
			result.add(split.getValue());   // add FileSplit
		}
		return result;
	}

	/**
	 * Add a file split to namespace
	 * @param fileName the filename which the file split belong to
	 * @param blockIndex block id for the file split
	 * @param fileSplit The file split object to store
	 */
	private synchronized void updateNameSpace(String fileName, int blockIndex, 
						FileSplit fileSplit) {
		Map<Integer, FileSplit> splits = null;
		if (nameSpace.containsKey(fileName)) {
			splits = nameSpace.get(fileName);
		} else {
			splits = new ConcurrentHashMap<Integer, FileSplit>();
		}
		splits.put(blockIndex, fileSplit);
		nameSpace.put(fileName, splits);
	}

	/**
	 * Return a copy of the files namespace
	 */
	@Override
	public Map<String, Set<FileSplit>> listAllFiles() {
		Map<String, Set<FileSplit>> copy = new ConcurrentHashMap<String, Set<FileSplit>>();
		for (Entry<String, Map<Integer, FileSplit>> file: nameSpace.entrySet()) {
			String fileName = file.getKey();
			Map<Integer, FileSplit> blocks = file.getValue();
			Set<FileSplit> splits = new HashSet<FileSplit>();
			for (Entry<Integer, FileSplit> split: blocks.entrySet()) {
				splits.add(split.getValue());
			}
			copy.put(fileName, new TreeSet<FileSplit>(splits));
		}
		return copy;
	}

	@Override
	public synchronized boolean containsFile(String path) {
		return nameSpace.containsKey(path);
	}

	/**
	 * Get all splits of a certain file.
	 * This function is called when JobTracker try to initialize tasks
	 */
	@Override
	public FileSplit[] getAllSplits(String path) throws RemoteException {
		// invalid input path
		if (!nameSpace.containsKey(path)) {
			return null;
		}
		Map<Integer, FileSplit> splitsMap = nameSpace.get(path);
		FileSplit[] splits = new FileSplit[splitsMap.size()];
		int index = 0;
		for (Map.Entry<Integer, FileSplit> entry: splitsMap.entrySet()) {
			splits[index++] = entry.getValue();
		}
		return splits;
	}

	@Override
	public void dataNodeOnline() throws RemoteException {
		// TODO Auto-generated method stub
		
	}
}
