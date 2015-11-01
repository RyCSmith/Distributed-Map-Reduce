package edu.upenn.cis455.mapreduce.worker;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;

import edu.upenn.cis455.mapreduce.Context;

/**
 * Provides output writing functionality for map functions
 * of map reduce jobs.
 */
public class MapperContext implements Context {
	
	ArrayList<PrintWriter> fileWriters;
	MessageDigest hashDigest;
	BigInteger modNum;
	WorkerServlet masterServlet;
	
	/**
	 * Default constructor. Creates a file for each active worker and opens
	 * a PrintWriter to each file when instantiated.
	 * @param workerNodeMap
	 * @param spoolOutDir
	 */
	public MapperContext(WorkerServlet masterServlet, 
			HashMap<String, String> workerNodeMap, String spoolOutDir) {
		this.masterServlet = masterServlet;
		try {
			hashDigest = MessageDigest.getInstance("SHA1");
		} catch (NoSuchAlgorithmException e1) {
			System.out.println("ERROR: Error creating hash scheme in MapperContext (MapperContext:39)");
		}
		fileWriters = new ArrayList<PrintWriter>();
		for (String key : workerNodeMap.keySet()) {
			try {
				File file = new File(spoolOutDir + "/" + key + ".txt");
				file.createNewFile();
				PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(file)));
				fileWriters.add(writer);
			} catch (IOException e) {
				System.out.println("ERROR: Error writing to file in MapperContext (MapperContext:49)");
			}
		}
		modNum = new BigInteger(new Integer(fileWriters.size()).toString());
		System.out.println("Context: Context created.");
	}
	
	/**
	 * Writes output to the proper file (determined using hash).
	 */
	@Override
	public synchronized void write(String key, String value) {
		int fileNum = getHash(key);
		PrintWriter outputFileWriter = fileWriters.get(fileNum);
		outputFileWriter.println(key + "\t" + value);
		masterServlet.incrementKeysWritten();
	}
	
	/**
	 * Creates an SHA-1 hash value for the given key, mod's it by numThreads
	 * and returns the index of the file that the line should be written to.
	 * @param key
	 * @return
	 */
	public int getHash(String key) {
		hashDigest.update(key.getBytes());
		byte[] output = hashDigest.digest();
     	BigInteger big = new BigInteger(output);
     	return big.mod(modNum).intValue();
	}
	
	/**
	 * Closes the writers that were opened for all files.
	 */
	public void closeWriters() {
		for (PrintWriter writer : fileWriters) {
			writer.close();
		}
	}

}
