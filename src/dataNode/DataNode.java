package dataNode;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import nameNode.NameNodeService;
import node.NodeID;
import configuration.Configuration;
import dfs.Service;

public class DataNode {

	private NodeID dataNodeID;

	public DataNode(String ip, int port, String rootDir) {
		this.dataNodeID = new NodeID(ip, port, rootDir);
	}

	public void bindService() {
		/* register dataNode service */
		DataNodeService dataNodeService = null;
		try {
			dataNodeService = new DataNodeServiceImpl();
			String name = "rmi://" + dataNodeID.toString() 
							+ "/DataNodeService";
			DataNodeService stub = 
					(DataNodeService) UnicastRemoteObject.exportObject(dataNodeService, 0);
			Registry registry = LocateRegistry.getRegistry();
//			Registry registry = LocateRegistry.createRegistry(1099);
			registry.rebind(name, stub);
			System.out.println("DataNodeService start!!");
		} catch (RemoteException e) {
			e.printStackTrace();
		}
		
	}
	
	/**
	 * Register the DataNode to NameNode
	 */
	public void registerToNameNode() {
		NodeID nameNodeID = new NodeID(Configuration.masterIP, Configuration.masterPort);
		try {
			Registry registry = LocateRegistry.getRegistry(nameNodeID.getIp());
			String name = "rmi://" + nameNodeID.toString() + "/NameNodeService";
			NameNodeService nameNodeService = (NameNodeService) registry.lookup(name);
			nameNodeService.registerDataNode(this.dataNodeID.getIp(), 
					this.dataNodeID.getPort(), this.dataNodeID.getRootPath());
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Stop DataNode
	 */
	public void stop() {
		System.exit(-1);
	}
	
	/* The main function is called when master node login on 
	 * slave node using ssh, and pass the args to the dataNode. */
	public static void main(String[] args) {
		try {
			// configuration file should be sent to slave node before
			Configuration.setup();
			// args should contain this datanode's ip and port and rootdir
			String ip = args[0];
			int port = Integer.parseInt(args[1]);
			String rootDir = args[2];
			DataNode dataNode = new DataNode(ip, port, rootDir);
			// register the dataNode to the NameNode
			dataNode.registerToNameNode();
			dataNode.bindService();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}