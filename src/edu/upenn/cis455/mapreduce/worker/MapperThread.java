package edu.upenn.cis455.mapreduce.worker;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import edu.upenn.cis455.mapreduce.Job;

public class MapperThread extends Thread {
	ArrayList<FileAssignment> fileAssignments;
	Job jobClassInstance;
	WorkerContext context;
	
	public MapperThread(ArrayList<FileAssignment> fileAssignments, Job jobClassInstance, WorkerContext context) {
		this.fileAssignments = fileAssignments;
		this.jobClassInstance = jobClassInstance;
		this.context = context;
	}
	
	public void run() {
		for (FileAssignment current : fileAssignments) {
			try {
				readAndProcessFA(current);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	private void readAndProcessFA(FileAssignment current) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(current.file));
		String line;
		if (current.entireFile) {
			while ((line = reader.readLine()) != null) {
				processSingleLine(line);
            }
		}
		else {
			int count = 1;
			while ((line = reader.readLine()) != null) {
				if (count >= current.startLine && count <= current.endLine)
					processSingleLine(line);
				count++;
			}
		}
		reader.close();
	}
	
	private void processSingleLine(String line) {
		int firstTabIndex = line.indexOf("\t");
        String key = line.substring(0, firstTabIndex);
        String value = line.substring(firstTabIndex + 1);
        jobClassInstance.map(key, value, context);
	}
	
}
