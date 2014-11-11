package dfs;

import java.io.IOException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.util.List;

public class RemoteBlockOperator {

	public void writeBlock(FileSplit fb, List<String> contents) throws Exception {
		// write to all servers. 
		for (String host: fb.getHosts()) {
			// lookup dataNode's rmi service
			String service = "rmi://" + host + "/DataNodeService";
			try {
				DataNodeService dataNodeService = (DataNodeService)Naming.lookup(service);
				dataNodeService.writeBlock(fb.getPath(host), contents);
			} catch (NotBoundException e) {
				e.printStackTrace();
			}
		}
	}

	public String[] readBlock(FileSplit fb) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

}
