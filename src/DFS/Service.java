package DFS;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public abstract class Service {
	/** get nameNodeService stub
	 * @param nameNodeID
	 * @return
	 */
	public static NameNodeService getNameNodeService(NodeID nameNodeID) {
		try {
			Registry registry = LocateRegistry.getRegistry(nameNodeID.getIp());
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
			Registry registry = LocateRegistry.getRegistry(dataNodeID.getIp());
			String name = "rmi://" + dataNodeID.toString() + "/DataNodeService";
			return (DataNodeService) registry.lookup(name);
		} catch (NotBoundException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * Register a dataNode to the NameNode
	 * @param nameNodeID
	 * @param dataNodeID
	 */
	public static void registerDataNode(NodeID nameNodeID, NodeID dataNodeID) {
		try {
			Registry registry = LocateRegistry.getRegistry(nameNodeID.getIp());
			String name = "rmi://" + nameNodeID.toString() + "/NameNodeService";
			NameNodeService nameNodeService = (NameNodeService) registry.lookup(name);
			nameNodeService.registerDataNode(dataNodeID.getIp(), 
						dataNodeID.getPort(), dataNodeID.getRootPath());
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		}
	}
}
