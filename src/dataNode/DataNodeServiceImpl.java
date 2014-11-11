package dataNode;

import java.rmi.RemoteException;
import java.util.List;

import fileSplit.LocalSplitOperator;

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
		LocalSplitOperator lbo = new LocalSplitOperator();
		lbo.writeBlock(filePath, contents);
	}

	@Override
	public List<String> readBLock(String filePath) throws Exception {
		LocalSplitOperator lbo = new LocalSplitOperator();
		System.out.println("Successfully get data from DataNode!!");
		return lbo.readBlock(filePath);
	}

}
