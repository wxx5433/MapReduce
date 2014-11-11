package dataNode;

import java.rmi.Remote;
import java.util.List;

public interface DataNodeService extends Remote {
	public void writeBlock(String filePath, List<String> contents) throws Exception;
	
	public List<String> readBLock(String filePath) throws Exception;
}
