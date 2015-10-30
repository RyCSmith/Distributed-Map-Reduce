package edu.upenn.cis455.mapreduce.worker;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import edu.upenn.cis455.mapreduce.Context;

public class WorkerContext implements Context {
	
	HashMap<String, File> workerFiles;
	
	/**
	 * Default constructor. Creates a file for each active worker when instantiated.
	 * @param workerNodeMap
	 * @param spoolOutDir
	 */
	public WorkerContext(HashMap<String, String> workerNodeMap, String spoolOutDir) {
		System.out.println("INCONTEXT: " + spoolOutDir);
		workerFiles = new HashMap<String, File>();
		for (String key : workerNodeMap.keySet()) {
			File file = new File(spoolOutDir + "/" + key + ".txt");
			try {
				file.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
			workerFiles.put(key, file);
		}
	}
	
	@Override
	public synchronized void write(String key, String value) {
		// TODO Auto-generated method stub
		
	}
	
	public synchronized void writeToFile(String key, String value) {
		
	}

}
