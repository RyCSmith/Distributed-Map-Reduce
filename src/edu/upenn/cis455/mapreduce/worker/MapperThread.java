package edu.upenn.cis455.mapreduce.worker;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import edu.upenn.cis455.mapreduce.Job;

public class MapperThread extends Thread {
	ArrayList<FileAssignment> fileAssignments;
	Job jobClassInstance;
	MapperContext context;
	WorkerServlet masterServlet;
	
	/**
	 * Default constructor. 
	 * @param fileAssignments - FileAssignments for this thread to process.
	 * @param jobClassInstance - Job class for this map reduce operation.
	 * @param context - Context object for emit'ing to files.
	 */
	public MapperThread(WorkerServlet masterServlet, 
			ArrayList<FileAssignment> fileAssignments, Job jobClassInstance, MapperContext context) {
		this.masterServlet = masterServlet;
		this.fileAssignments = fileAssignments;
		this.jobClassInstance = jobClassInstance;
		this.context = context;
	}
	
	/**
	 * Main execution. Processes each FileAssignment given to this thread.
	 */
	public void run() {
		for (FileAssignment current : fileAssignments) {
			try {
				readAndProcessFA(current);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Processes each line of a given FileAssignment.
	 * @param current
	 * @throws IOException
	 */
	private void readAndProcessFA(FileAssignment current) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(current.file));
		String line;
		if (current.entireFile) {
			while ((line = reader.readLine()) != null) {
				processSingleLine(line);
				masterServlet.incrementKeysRead();
            }
		}
		else {
			int count = 1;
			while ((line = reader.readLine()) != null) {
				if (count >= current.startLine && count <= current.endLine) {
					processSingleLine(line);
					masterServlet.incrementKeysRead();
				}
				count++;
			}
		}
		reader.close();
	}
	
	/**
	 * Parses a given single line and calls the jobs class's map() function.
	 * @param line
	 */
	private void processSingleLine(String line) {
		int firstTabIndex = line.indexOf("\t");
        String key = line.substring(0, firstTabIndex);
        String value = line.substring(firstTabIndex + 1);
        jobClassInstance.map(key, value, context);
	}
	
}
