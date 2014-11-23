package dataNode;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import nameNode.NameNodeService;
import node.NodeID;
import configuration.Configuration;

public class DataNode {

	private NodeID dataNodeID;
	private Configuration configuration;
	private NameNodeService nameNodeService;
	private Thread heartBeat;
	private HeartBeatThread heartBeatThread;

	public DataNode() {
		configuration = new Configuration();
		String ip = null;
		try {
			ip = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			System.out.println("Fail to get data node's ip address");
			e.printStackTrace();
		}
		this.dataNodeID = new NodeID(ip, configuration.dataNodePort);
		File localDir = new File(dataNodeID.getLocalPath());
		localDir.mkdirs();
		File dfsDir = new File(dataNodeID.getDFSPath());
		dfsDir.mkdirs();
	}

	public DataNode(String localPath, String dfsPath) {
		configuration = new Configuration();
		String ip = null;
		try {
			ip = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			System.out.println("Fail to get data node's ip address");
			e.printStackTrace();
		}
		this.dataNodeID = new NodeID(dfsPath, localPath, ip,
				configuration.dataNodePort);
		File localDir = new File(dataNodeID.getLocalPath());
		localDir.mkdirs();
		File dfsDir = new File(dataNodeID.getDFSPath());
		dfsDir.mkdirs();
	}

	public void bindService() {
		/* register dataNode service */
		DataNodeService dataNodeService = null;
		try {
			dataNodeService = new DataNodeServiceImpl();
			String name = "rmi://" + dataNodeID.toString() + "/DataNodeService";
			DataNodeService stub = (DataNodeService) UnicastRemoteObject
					.exportObject(dataNodeService, 0);
			Registry registry = null;
			try {
				registry = LocateRegistry.getRegistry(configuration.rmiPort);
				registry.rebind(name, stub);
			} catch (Exception e) {
				registry = LocateRegistry.createRegistry(configuration.rmiPort);
				registry.rebind(name, stub);
			}
			System.out.println("DataNodeService start!!");
		} catch (RemoteException e) {
			e.printStackTrace();
		}

	}

	/**
	 * Register the DataNode to NameNode
	 */
	public void registerToNameNode() {
		NodeID nameNodeID = new NodeID(configuration.nameNodeIP,
				configuration.nameNodePort);
		try {
			Registry registry = LocateRegistry.getRegistry(nameNodeID.getIp(),
					configuration.rmiPort);
			String name = "rmi://" + nameNodeID.toString() + "/NameNodeService";
			nameNodeService = (NameNodeService) registry.lookup(name);
			nameNodeService.registerDataNode(dataNodeID);
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		}
	}

	private void startHeartBeat() {
		heartBeatThread = new HeartBeatThread(this);
		heartBeat = new Thread(heartBeatThread);
		heartBeat.start();
	}

	public void sendHeartbeat() throws RemoteException {
		String response = nameNodeService.updateDataNodeStatus(this.dataNodeID);
	}

	/**
	 * Stop DataNode
	 */
	public void stop() {
		System.exit(-1);
	}

	/*
	 * The main function is called when master node login on slave node using
	 * ssh, and pass the args to the dataNode.
	 */
	public static void main(String[] args) {

		DataNode dataNode;
		if (args.length == 2)
			dataNode = new DataNode(args[0], args[1]);
		else
			dataNode = new DataNode();
		// register the dataNode to the NameNode
		dataNode.registerToNameNode();
		dataNode.bindService();
		dataNode.startHeartBeat();
	}

}
