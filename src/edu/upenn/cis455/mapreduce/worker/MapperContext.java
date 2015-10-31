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
			e1.printStackTrace();
		}
		fileWriters = new ArrayList<PrintWriter>();
		for (String key : workerNodeMap.keySet()) {
			try {
				File file = new File(spoolOutDir + "/" + key + ".txt");
				file.createNewFile();
				PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(file)));
				fileWriters.add(writer);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		modNum = new BigInteger(new Integer(fileWriters.size()).toString());
		System.out.println("Context: context created");
	}
	
	@Override
	public synchronized void write(String key, String value) {
		int fileNum = getHash(key);
		PrintWriter outputFileWriter = fileWriters.get(fileNum);
		outputFileWriter.println(key + "\t" + value);
		masterServlet.incrementKeysWritten();
	}
	
	public int getHash(String key) {
		hashDigest.update(key.getBytes());
		byte[] output = hashDigest.digest();
     	BigInteger big = new BigInteger(output);
     	return big.mod(modNum).intValue();
	}
	
	public void closeWriters() {
		System.out.println("Context: writers being closed.");
		for (PrintWriter writer : fileWriters) {
			writer.close();
		}
	}

}
