package Configuration;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

public abstract class Configuration {
	public static String masterIP;
	public static int masterPort;
	public static List<String> slaveIPList = new ArrayList<String>();
	public static List<Integer> slavePortList = new ArrayList<Integer>();
	public static List<String> rootDir = new ArrayList<String>();
	public static int mapSlots;
	public static int reduceSlots;
	public static int splitSize;
	public static int replicaNum;
	
	public static void setup() throws Exception {
		FileReader fr = new FileReader("config");
		BufferedReader br = new BufferedReader(fr);
		
		String line = null;
		String[] keyValue = new String[2];
		while ((line = br.readLine()) != null) {
			keyValue = line.split("=");
			if (keyValue[0].trim().equals("")) {  // skip empty line
				continue;
			}
			String key = keyValue[0].trim();
			String value = keyValue[1].trim();
			
			if (key.equals("masterIP")) {
				masterIP = value;
			} else if (key.equals("masterPort")) {
				masterPort = Integer.parseInt(value);
			} else if (key.equals("slaveIP")) {
				slaveIPList.add(value);
			} else if (key.equals("slavePort")) {
				slavePortList.add(Integer.parseInt(value));
			} else if (key.equals("mapSlots")) {
				mapSlots = Integer.parseInt(value);
			} else if (key.equals("reduceSlots")) {
				reduceSlots = Integer.parseInt(value);
			} else if (key.equals("splitSize")) {
				splitSize = Integer.parseInt(value);
			} else if (key.equals("replicaNum")) {
				replicaNum = Integer.parseInt(value);
			}else if (key.equals("rootDir")) {
				rootDir.add(value);
			} else {
				br.close();
				throw new Exception("Undefined key-value in config file");
			}
		}
		br.close();
		if (slavePortList.size() != slaveIPList.size()) {
			throw new Exception("Number of slaveIP and slavePort does not match!");
		}
	}
	
	public static void main(String[] args) {
		try {
			Configuration.setup();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
