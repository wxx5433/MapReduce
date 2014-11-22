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
import dataNode.HeartBeat;
import fileSplit.FileSplit;

public class NameNodeServiceImpl implements NameNodeService {

	private Configuration configuration;
	private NameNode nameNode;

	public NameNodeServiceImpl(Configuration configuration, NameNode nameNode) 
			throws RemoteException {
		super();
		this.configuration = configuration;
		this.nameNode = nameNode;
	}

	/**
	 * add a dataNode to management
	 */
	@Override
	public void registerDataNode(NodeID dataNodeID) throws RemoteException {
		nameNode.registerDataNode(dataNodeID);
		System.out.println("New dataNode registered!!:" + dataNodeID.toString());
		System.out.println("Path: " + dataNodeID.getDFSPath());
	}

	@Override
	public synchronized Iterable<NodeID> getDataNodesToUpload(String fileName, int blockIndex) {
		List<NodeID> dataNodeIDs = new ArrayList<NodeID>();
		// in case of uploading the same file split
		synchronized (this) {
			if (nameNode.containsSplit(fileName, blockIndex)) { 
				return dataNodeIDs;
			}
		}
		FileSplit fileSplit = new FileSplit(configuration, fileName, blockIndex);
		int count = 0;
		while (count < configuration.replicaNum && nameNode.hasNode()) {
			dataNodeIDs.add(nameNode.getNode());
			++count;
		}
		for (NodeID nodeID: dataNodeIDs) {
			try {
				fileSplit.addHost(nodeID.toString(), nodeID.getDFSPath() 
						+ File.separator + fileName + "_" + blockIndex);
			} catch (Exception e) {
				e.printStackTrace();
			}
			nodeID.incrementBlockCount();
			nameNode.addNode(nodeID);
		}
		// Actually, we need to get response from dataNodes to update
		nameNode.updateNameSpace(fileName, blockIndex, fileSplit);
//		System.out.println("Succesfully get dataNode info from NameNode!");
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
		if (!nameNode.containsFile(fileName)) {
			return result;
		}
		FileSplit[] splits = nameNode.getAllSplits(fileName);
		for (FileSplit split: splits) {
			result.add(split);   // add FileSplit
		}
		return result;
	}

	/**
	 * Return a copy of the files namespace
	 */
	@Override
	public Map<String, Set<FileSplit>> listAllFiles() {
		Map<String, Set<FileSplit>> copy = new ConcurrentHashMap<String, Set<FileSplit>>();
		for (Entry<String, Map<Integer, FileSplit>> file: nameNode.getAllFiles()) {
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
		return nameNode.containsFile(path);
	}

	/**
	 * Get all splits of a certain file.
	 * This function is called when JobTracker try to initialize tasks
	 */
	@Override
	public FileSplit[] getAllSplits(String path) throws RemoteException {
		return nameNode.getAllSplits(path);
	}

	@Override
	public HeartBeatResponse updateDataNodeStatus(HeartBeat heartBeat)
			throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}
}
