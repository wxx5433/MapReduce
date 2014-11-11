package DFS;

import java.rmi.RemoteException;
import java.util.List;

public class DataNodeServiceImpl implements DataNodeService {

	protected DataNodeServiceImpl() throws RemoteException {
		super();
	}

	/**
	 * This function will run on DataNode locally, 
	 * so we only need to use localBlockWriter.
	 */
	@Override
	public void writeBlock(String filePath, List<String> contents) throws Exception {
		LocalBlockOperator lbo = new LocalBlockOperator();
		lbo.writeBlock(filePath, contents);
	}

	@Override
	public List<String> readBLock(String filePath) throws Exception {
		LocalBlockOperator lbo = new LocalBlockOperator();
		System.out.println("Successfully get data from DataNode!!");
		return lbo.readBlock(filePath);
	}

}
