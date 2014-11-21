package dataNode;

import java.rmi.RemoteException;
import java.util.List;

import configuration.Configuration;
import fileSplit.LocalSplitOperator;

public class DataNodeServiceImpl implements DataNodeService {

	private Configuration configuration;
	
	protected DataNodeServiceImpl(Configuration configuration) throws RemoteException {
		super();
		this.configuration = configuration;
	}

	/**
	 * This function will run on DataNode locally, 
	 * so we only need to use localBlockWriter.
	 */
	@Override
	public void writeSplit(String filePath, List<String> contents) throws Exception {
		LocalSplitOperator lbo = new LocalSplitOperator(configuration);
		lbo.writeSplit(filePath, contents);
	}

	@Override
	public List<String> readSplit(String filePath) throws Exception {
		LocalSplitOperator lbo = new LocalSplitOperator(configuration);
		System.out.println("Successfully get data from DataNode!!");
		return lbo.readSplit(filePath);
	}

}
