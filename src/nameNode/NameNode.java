package nameNode;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import node.NodeID;
import configuration.Configuration;

public class NameNode {
	
	private NodeID nameNodeID;
	private NameNodeService nameNodeService; 
	private Configuration configuration;
	
	public NameNode(Configuration configuration) {
		nameNodeID = new NodeID(configuration.nameNodeIP, configuration.nameNodePort);
	}
	
	public void bindService() {
		try {
			nameNodeService = new NameNodeServiceImpl(configuration);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
		try {
			String name = "rmi://" + nameNodeID.toString() 
							+ "/NameNodeService";
			NameNodeService stub = 
					(NameNodeService) UnicastRemoteObject.exportObject(nameNodeService, 0);
			Registry registry = LocateRegistry.createRegistry(1099);
			registry.rebind(name, stub);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
		System.out.println("NameNode service started!!");
	}
	
	/**
	 * Stop the NameNode
	 */
	public void stop() {
		System.exit(-1);
	}
	
	public static void main(String[] args) {
		Configuration configuration = new Configuration();
		NameNode nameNode = new NameNode(configuration);
		nameNode.bindService();
	}
}
