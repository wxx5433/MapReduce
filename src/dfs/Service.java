package dfs;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import configuration.Configuration;
import dataNode.DataNodeService;
import nameNode.NameNodeService;
import node.NodeID;

public abstract class Service {
	/** get nameNodeService stub
	 * @param nameNodeID
	 * @return
	 */
	public static NameNodeService getNameNodeService(NodeID nameNodeID) {
		try {
			Registry registry = LocateRegistry.getRegistry(nameNodeID.getIp(), 
					new Configuration().rmiPort);
			String name = "rmi://" + nameNodeID.toString() + "/NameNodeService";
			return (NameNodeService) registry.lookup(name);
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		}
		return null;
	}

	/** get dataNodeService stub
	 * @param dataNodeID
	 * @return
	 */
	public static DataNodeService getDataNodeService(NodeID dataNodeID) {
		try {
			Registry registry = LocateRegistry.getRegistry(dataNodeID.getIp(), 
					new Configuration().rmiPort);
			String name = "rmi://" + dataNodeID.toString() + "/DataNodeService";
			return (DataNodeService) registry.lookup(name);
		} catch (NotBoundException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
}
