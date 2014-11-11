package configuration;

import java.io.BufferedReader;
import java.io.FileReader;

public class MyConfiguration {
	public static String masterIP;
	public static int masterPort;
	public static int slavePort;
	public static int RECORD_PER_MAP;
	public static int reduceNum;
	public static int REP_FACTOR;
	public static int heartBeatTimeout;
	public static int heartBeatInterval;
	public static int splitBlockLinesNum;

	private static MyConfiguration myConfiguration = new MyConfiguration();

	public static void setup() throws Exception {
		FileReader fr = new FileReader("config");
		BufferedReader br = new BufferedReader(fr);

		String line;
		String[] keyValue = new String[2];
		while ((line = br.readLine()) != null) {
			keyValue = line.split("=");
			if (keyValue[0].equals(""))
				continue;
			keyValue[0] = keyValue[0].trim();
			keyValue[1] = keyValue[1].trim();
			if (keyValue[0].equals("masterIP"))
				masterIP = keyValue[1];
			if (keyValue[0].equals("masterPort"))
				masterPort = Integer.parseInt(keyValue[1]);
			if (keyValue[0].equals("slavePort"))
				slavePort = Integer.parseInt(keyValue[1]);
			if (keyValue[0].equals("RECORD_PER_MAP"))
				RECORD_PER_MAP = Integer.parseInt(keyValue[1]);
			if (keyValue[0].equals("reduceNum"))
				reduceNum = Integer.parseInt(keyValue[1]);
			if (keyValue[0].equals("REPLICA_FACTOR"))
				REP_FACTOR = Integer.parseInt(keyValue[1]);
			if (keyValue[0].equals("heartBeatTimeout"))
				heartBeatTimeout = Integer.parseInt(keyValue[1]);
			if (keyValue[0].equals("heartBeatInterval"))
				heartBeatInterval = Integer.parseInt(keyValue[1]);

		}
		br.close();
	}

	public static MyConfiguration getInstance() {
		return myConfiguration;
	}
}
