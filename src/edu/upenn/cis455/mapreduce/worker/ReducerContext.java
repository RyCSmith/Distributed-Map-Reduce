package edu.upenn.cis455.mapreduce.worker;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import edu.upenn.cis455.mapreduce.Context;

/**
 * Provides output writing functionality for reduce functions
 * of map reduce jobs.
 */
public class ReducerContext implements Context {

	PrintWriter outputWriter;
	WorkerServlet masterServlet;
	
	/**
	 * Default constructor.
	 * Creates an output file with the requested path.
	 * Deletes first if this file already exists.
	 * @param outputDir
	 */
	public ReducerContext(WorkerServlet masterServlet, String outputDir) {
		this.masterServlet = masterServlet;
		try {
			File outputFile = makeOutputFile(outputDir);
			outputFile.createNewFile();
			outputWriter = new PrintWriter(new BufferedWriter(new FileWriter(outputFile)));
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("Reducer Context: context created");
	}
	
	/**
	 * Appends a line (key and value separated by tab) to the output file.
	 */
	@Override
	public synchronized void write(String key, String value) {
		outputWriter.println(key + "\t" + value);
		masterServlet.incrementKeysWritten();
	}
	
	public void closeWriter() {
		try {
			outputWriter.close();
		} catch (Exception e) { }
	}
	
	/**
	 * Creates a file with a unique name to write results to.
	 * @param outputDir
	 * @return
	 */
	public File makeOutputFile(String outputDir) {
		String fullPath = getFullPath(outputDir);
		File outputFile = new File(fullPath);
		while (outputFile.exists())
			outputFile = new File(getFullPath(outputDir));
		return outputFile;
	}
	
	/**
	 * Creates a unique file path that does not yet exist.
	 * @param outputDir
	 * @return
	 */
	public String getFullPath(String outputDir) {
		String fullPath;
		if (outputDir.endsWith("/"))
			fullPath = outputDir + "results-" + masterServlet.jobClass + "-" + System.currentTimeMillis() + ".txt";
		else
			fullPath = outputDir + "/results-" + masterServlet.jobClass + "-" + System.currentTimeMillis() + ".txt";
		return fullPath;
	}
}
