package DFS;

import java.io.IOException;

public interface BlockOperator {
	
	// dataNode has already know the path to write
	public void writeBlock(String path, String[] contents) throws IOException, Exception;
	
	public String[] readBlock(String filePath) throws Exception;
}
