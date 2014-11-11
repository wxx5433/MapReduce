package dfs;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import nameNode.NameNodeService;
import node.NodeID;
import configuration.Configuration;
import dataNode.DataNodeService;
import fileSplit.FileSplit;

public class DFSClient {

	private NameNodeService nameNodeService = null;

	public DFSClient() {
		NodeID nameNodeID = new NodeID(Configuration.masterIP, Configuration.masterPort);
		nameNodeService = Service.getNameNodeService(nameNodeID);
	}

	/**
	 * Upload file to DFS
	 * @param filePath local path of the upload file
	 * @param fileName the file to upload
	 */
	public void uploadFile(String filePath, String fileName) {
		FileReader fr  = null;
		BufferedReader br = null;
		int splitSize = Configuration.splitSize;
		try {
			fr = new FileReader(filePath + File.separator + fileName);
			br = new BufferedReader(fr);
			String line;
			int lineCount = 0;
			Iterable<NodeID> dataNodes = null;
			List<String> contents = new ArrayList<String>();
			while ((line = br.readLine()) != null) {
				++lineCount;
				contents.add(line);
				if (lineCount % splitSize == 0) {
					int blockIndex = lineCount / splitSize;
					dataNodes = nameNodeService.getDataNodesToUpload(fileName, blockIndex);
					for (NodeID dataNodeID: dataNodes) {
						DataNodeService dataNodeService = Service.getDataNodeService(dataNodeID);
						String dataNodePath = dataNodeID.getRootPath() + File.separator + 
								fileName + "_" + blockIndex;
						try {
							dataNodeService.writeBlock(dataNodePath, contents);
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
					contents.clear();
				}
				// should send response to NameNode here to confirm data transfer success. 
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				br.close();
				fr.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Download file from DFS to client
	 * @param fileName The name of the file to download
	 * @param localPath Local path where the download file should be put.
	 */
	public void getFile(String fileName, String localPath, String targetName) {
		NodeID nameNodeID = new NodeID(Configuration.masterIP, Configuration.masterPort);
		NameNodeService nameNodeService = Service.getNameNodeService(nameNodeID);
		Iterable<FileSplit> splits = null;
		try {
			splits = nameNodeService.getDataNodesToDownload(fileName);
		} catch (RemoteException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		FileWriter fw = null;
		BufferedWriter bw = null;
		try {
			fw = new FileWriter(localPath + File.separator + targetName);
			bw = new BufferedWriter(fw);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		boolean firstLine = true;
		for (FileSplit split: splits) {
			NodeID dataNodeID = split.getOneHost();
			DataNodeService dataNodeService = Service.getDataNodeService(dataNodeID);
			try {
				List<String> lines = dataNodeService.readBLock(
							split.getPath(dataNodeID.toString()));
				for (String line: lines) {
					if (firstLine) {
						bw.write(line);
						firstLine = false;
					} else {
						bw.write("\n" + line);
					}
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		try {
			bw.close();
			fw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * List all files on DFS 
	 */
	public void listAllFiles() {
		Map<String, Set<FileSplit>> filesCopy = null;
		try {
			filesCopy = nameNodeService.listAllFiles();
		} catch (RemoteException e) {
			e.printStackTrace();
		}
		System.out.println("List all files on the DFS:");
		for (Map.Entry<String, Set<FileSplit>> file: filesCopy.entrySet()) {
			String fileName = file.getKey();
			System.out.println("filename: " + fileName);
			for (FileSplit split: file.getValue()) {
				System.out.println("\tSplit " + split.getBlockIndex() + ":");
				for (String host: split.getHosts()) {
					System.out.println("\t\t" + host + "\t" + split.getPath(host));
				}
			}
		}
	}

	public static void main(String[] args) {
		try {
			Configuration.setup();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		DFSClient client = new DFSClient();
		client.uploadFile(".", "test");
		client.listAllFiles();
		client.getFile("test", ".", "result");
	}
}
