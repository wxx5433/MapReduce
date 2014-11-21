package nameNode;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Map;
import java.util.Set;

import node.NodeID;
import fileSplit.FileSplit;

public interface NameNodeService extends Remote {

	public void registerDataNode(String ip, int port, String rootPath) throws RemoteException;
	
	public Iterable<NodeID> getDataNodesToUpload(String fileName, int blockIndex) throws RemoteException;
	
	public Map<String, Set<FileSplit>>  listAllFiles() throws RemoteException;
		
	public Iterable<FileSplit> getDataNodesToDownload(String fileName) throws RemoteException;
	
	public boolean containsFile(String path) throws RemoteException;
	
	public FileSplit[] getAllSplits(String path) throws RemoteException;
	
	public void dataNodeOnline() throws RemoteException;
	
}
