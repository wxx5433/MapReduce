package nameNode;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;

import node.NodeID;
import configuration.Configuration;
import fileSplit.FileSplit;

public class NameNode {

	private NodeID nameNodeID;
	private NameNodeService nameNodeService; 
	private Configuration configuration;
	private Queue<NodeID> nodes;
	private Map<String, Map<Integer, FileSplit>> nameSpace;

	public NameNode() {
		configuration = new Configuration();
		nameNodeID = new NodeID(configuration.nameNodeIP, configuration.nameNodePort);
		this.nodes = new PriorityBlockingQueue<NodeID>();
		this.nameSpace = new ConcurrentHashMap<String, Map<Integer, FileSplit>>();
	}

	public void bindService() {
		try {
			nameNodeService = new NameNodeServiceImpl(configuration, this);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
		try {
			String name = "rmi://" + nameNodeID.toString() 
					+ "/NameNodeService";
			NameNodeService stub = 
					(NameNodeService) UnicastRemoteObject.exportObject(nameNodeService, 0);
			try {
				Registry registry = LocateRegistry.getRegistry();
				registry.rebind(name, stub);
			} catch (Exception e) {
				Registry registry = LocateRegistry.createRegistry(1099);
				registry.rebind(name, stub);
			}
		} catch (RemoteException e) {
			e.printStackTrace();
		}
		System.out.println("NameNode service started!!");
	}

	public void registerDataNode(NodeID dataNodeID) {
		nodes.offer(dataNodeID);
	}

	public synchronized boolean containsFile(String path) {
		return nameSpace.containsKey(path);
	}

	public synchronized boolean containsSplit(String path, int splitIndex) {
		if (!nameSpace.containsKey(path)) {
			return false;
		}
		return nameSpace.get(path).containsKey(splitIndex);
	}

	public synchronized FileSplit[] getAllSplits(String fileName) {
		if (!containsFile(fileName)) {
			return null;
		}
		Map<Integer, FileSplit> splitsMap = nameSpace.get(fileName);
		FileSplit[] splits = new FileSplit[splitsMap.size()];
		int index = 0;
		for (Map.Entry<Integer, FileSplit> entry: splitsMap.entrySet()) {
			splits[index++] = entry.getValue();
		}
		return splits;
	}

	public synchronized Map<Integer, FileSplit> getSplitsMap(String fileName) {
		if (!containsFile(fileName)) {
			return null;
		}
		return nameSpace.get(fileName);
	}

	public synchronized Set<Entry<String, Map<Integer, FileSplit>>> getAllFiles() {
		return nameSpace.entrySet();
	}

	/**
	 * Add a file split to namespace
	 * @param fileName the filename which the file split belong to
	 * @param blockIndex block id for the file split
	 * @param fileSplit The file split object to store
	 */
	public synchronized void updateNameSpace(String fileName, int blockIndex, 
			FileSplit fileSplit) {
	Map<Integer, FileSplit> splits = getSplitsMap(fileName);
		if (splits == null) {
			splits = new ConcurrentHashMap<Integer, FileSplit>();
		}
		splits.put(blockIndex, fileSplit);
		nameSpace.put(fileName, splits);
	}

	public synchronized boolean hasNode() {
		return !nodes.isEmpty();
	}

	public synchronized NodeID getNode() {
		return nodes.poll();
	}

	public synchronized void addNode(NodeID nodeID) {
		nodes.add(nodeID);
	}

	/**
	 * Stop the NameNode
	 */
	public void stop() {
		System.exit(-1);
	}

	public static void main(String[] args) {
		NameNode nameNode = new NameNode();
		nameNode.bindService();
	}
}
