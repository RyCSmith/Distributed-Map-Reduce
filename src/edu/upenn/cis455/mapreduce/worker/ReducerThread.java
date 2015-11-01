package edu.upenn.cis455.mapreduce.worker;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashSet;

import edu.upenn.cis455.mapreduce.Job;

public class ReducerThread extends Thread {

	FileAssignment fileAssignment;
	Job jobClassInstance;
	ReducerContext context;
	WorkerServlet masterServlet;
	
	/**
	 * Default constructor.
	 * @param fileAssignment
	 * @param jobClassInstance
	 * @param context
	 */
	public ReducerThread(WorkerServlet masterServlet, 
			FileAssignment fileAssignment, Job jobClassInstance, ReducerContext context) {
		this.masterServlet = masterServlet;
		this.fileAssignment = fileAssignment;
		this.jobClassInstance = jobClassInstance;
		this.context = context;
	}
	
	/**
	 * Executes main functionality of the thread. Reads a fileAssingment, processes into
	 * format (key, array of values) and submits each group for reducing.
	 */
	public void run() {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(fileAssignment.file));
			ArrayList<String> valueSet = null;
			String currentKey = null;
			
			//loop through each line in the FileAssignment, gather all values for a given key and submit to reduce function
			int lineCount = 0;
			String line;
			while((line = reader.readLine()) != null) {
				lineCount++;
				KeyValPair parsedLine = new KeyValPair(line);
				if (fileAssignment.entireFile ||
						(lineCount >= fileAssignment.startLine && lineCount <= fileAssignment.endLine)) {
					masterServlet.incrementKeysRead();
					if (currentKey == null) {
						currentKey = parsedLine.key;
						valueSet = new ArrayList<String>();
						valueSet.add(parsedLine.value);
					}
					else if (currentKey.equalsIgnoreCase(parsedLine.key)) {
						valueSet.add(parsedLine.value);
					}
					else {
						submitToReduce(currentKey, valueSet);
						valueSet = new ArrayList<String>();
						currentKey = parsedLine.key;
						valueSet.add(parsedLine.value);
					}
				}
			}
			//submit job that was in progress while file ended
			submitToReduce(currentKey, valueSet);
			reader.close();
		} catch (Exception e) {
			System.out.println("ERROR: Error in reducer thread " + this.getName() + " (ReducerThread:71)");
		}
	}
	
	/**
	 * Converts an ArrayList of String values into a String[] and submits to 
	 * jobInstanceClass for reducing.
	 * @param key
	 * @param values
	 */
	public void submitToReduce(String key, ArrayList<String> values) {
		String[] valuesArray = new String[values.size()];
        values.toArray(valuesArray);
        jobClassInstance.reduce(key, valuesArray, context);
	}
	
	/**
	 * Inner class, takes a line read from a file, parses into a key and value 
	 * and makes those values accessible via the object instance.
	 *
	 */
	public class KeyValPair {
		String key;
		String value;
		public KeyValPair(String line) {
			parseLine(line);
		}
		
		/**
		 * Splits a line on the first instance of a tab and holds the values.
		 * @param line
		 */
		void parseLine(String line) {
			int firstTabIndex = line.indexOf("\t");
	        key = line.substring(0, firstTabIndex).trim();
	        value = line.substring(firstTabIndex + 1).trim();
		}
	}
}
