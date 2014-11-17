package fileSplit;

import java.io.File;
import java.io.IOException;
import java.util.List;

import node.NodeID;
import dataNode.DataNodeService;
import dfs.Service;

public class RemoteSplitOperator {
	/**
	 * Write splits to several remote dataNodes
	 * 
	 * @param dataNodes
	 *            The dataNodes to write the file split
	 * @param fileName
	 *            The file's name which the split belong to
	 * @param blockIndex
	 *            The block index for the split
	 * @param contents
	 *            The split's contents
	 * @throws Exception
	 */
	public void writeSplit(Iterable<NodeID> dataNodes, String fileName,
			int blockIndex, List<String> contents) throws Exception {
		for (NodeID dataNodeID : dataNodes) {
			DataNodeService dataNodeService = Service
					.getDataNodeService(dataNodeID);
			String dataNodePath = dataNodeID.getRootPath() + File.separator
					+ fileName + "_" + blockIndex;
			try {
				dataNodeService.writeSplit(dataNodePath, contents);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public List<String> readSplit(FileSplit split, String localPath,
			String targetName) throws IOException {
		NodeID dataNodeID = split.getOneDataNode();
		DataNodeService dataNodeService = Service
				.getDataNodeService(dataNodeID);
		List<String> lines = null;
		try {
			// read split from dataNode. It's an RMI
			lines = dataNodeService.readSplit(split.getPath(dataNodeID
					.toString()));
		} catch (Exception e) {
			System.out.println("Fail to download split from dataNode");
			e.printStackTrace();
		}
		return lines;
	}

	public List<String> readBlock(NodeID dataNodeID, String localPath,
			String targetName) throws IOException {
		DataNodeService dataNodeService = Service
				.getDataNodeService(dataNodeID);
		List<String> lines = null;
		try {
			// read split from dataNode. It's an RMI
			lines = dataNodeService.readSplit(localPath);
		} catch (Exception e) {
			System.out.println("Fail to download split from dataNode");
			e.printStackTrace();
		}
		return lines;
	}

}
