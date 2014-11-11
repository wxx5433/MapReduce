package DFS;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Map;
import java.util.Set;

public interface NameNodeService extends Remote {

	public void registerDataNode(String ip, int port, String rootPath) throws RemoteException;
	
	public Iterable<NodeID> getDataNodesToUpload(String fileName, int blockIndex) throws RemoteException;
	
	public Map<String, Set<FileSplit>>  listAllFiles() throws RemoteException;
		
	
}
