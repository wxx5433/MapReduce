package configuration;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

public class Configuration {
	public String taskTrackerIP;
	public int taskTrackerPort;
	public String nameNodeIP;
	public int nameNodePort;
	public int dataNodePort;
	public int mapSlots;
	public int reduceSlots;
	public int splitSize;
	public int replicaNum;
	
	public Configuration() {
		try {
			setup();
		} catch (Exception e) {
			System.out.println("invalid configuration file");
			e.printStackTrace();
		}
	}
	
	public void setup() throws Exception {
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
			
			if (key.equals("taskTrackerIP")) {
				taskTrackerIP = value;
			} else if (key.equals("taskTrackerPort")) {
				taskTrackerPort = Integer.parseInt(value);
			} else if (key.equals("nameNodeIP")) {
				nameNodeIP = value;
			} else if (key.equals("nameNodePort")) {
				nameNodePort = Integer.parseInt(value);
			} else if (key.equals("dataNodePort")) {
				dataNodePort = Integer.parseInt(value);
			} else if (key.equals("mapSlots")) {
				mapSlots = Integer.parseInt(value);
			} else if (key.equals("reduceSlots")) {
				reduceSlots = Integer.parseInt(value);
			} else if (key.equals("splitSize")) {
				splitSize = Integer.parseInt(value);
			} else if (key.equals("replicaNum")) {
				replicaNum = Integer.parseInt(value);
			} else {
				br.close();
				throw new Exception("Undefined key-value in config file");
			}
		}
		br.close();
	}
}
