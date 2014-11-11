package dataNode;

import java.rmi.Remote;
import java.util.List;

public interface DataNodeService extends Remote {
	public void writeSplit(String filePath, List<String> contents) throws Exception;
	
	public List<String> readSplit(String filePath) throws Exception;
}
