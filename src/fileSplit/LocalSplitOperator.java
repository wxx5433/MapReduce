package fileSplit;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;

import configuration.Configuration;

public abstract class LocalSplitOperator {

	private final static Configuration configuration = new Configuration();

	/**
	 * This method will be invoked on the DataNode to write a block to its local
	 * disk.
	 */
	public static void writeSplit(String filePath, List<String> contents)
			throws Exception {
		if (contents.size() > configuration.splitSize) {
			throw new Exception("Write too many lines to a block!");
		}
		FileWriter fw = new FileWriter(filePath);
		BufferedWriter bw = new BufferedWriter(fw);
		System.out.println(filePath);
		for (String line : contents) {
			System.out.println(line);
			bw.write(line + "\n");
		}
		bw.flush();
		bw.close();
		fw.close();
		System.out.println("Data written to dataNode's local disk!");
	}

	/**
	 * This method will be invoked by a dataNode to read a block from its local
	 * disk.
	 */
	public static List<String> readSplit(String filePath) throws Exception {
		FileReader fr = new FileReader(filePath);
		BufferedReader br = new BufferedReader(fr);
		String line;
		List<String> result = new ArrayList<String>();
		while ((line = br.readLine()) != null) {
			result.add(line);
		}
		br.close();
		fr.close();
		// if (result.size() > configuration.splitSize) {
		// throw new Exception("There are too many lines in the block!");
		// }
		return result;
	}
}
