package DFS;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import Configuration.Configuration;

public class NameNode {
	
	private NodeID nameNodeID;
	private NameNodeService nameNodeService; 
	
	public NameNode(String ip, int port) {
		nameNodeID = new NodeID(ip, port);
	}
	
	public void bindService() {
		try {
			nameNodeService = new NameNodeServiceImpl();
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
	
	public static void main(String[] args) {
		try {
			Configuration.setup();
		} catch (Exception e) {
			System.out.println("MasterNode setup failure");
			System.exit(-1);
		}
		NameNode nameNode = new NameNode(Configuration.masterIP, Configuration.masterPort);
		nameNode.bindService();
	}
}
