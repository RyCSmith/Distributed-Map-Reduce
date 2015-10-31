package edu.upenn.cis455.mapreduce.worker;

import edu.upenn.cis455.mapreduce.Job;

public class ReducerThread extends Thread {

	FileAssignment fileAssignment;
	Job jobClass;
	ReducerContext context;
	
	public ReducerThread(FileAssignment fileAssignment, Job jobClass, ReducerContext context) {
		this.fileAssignment = fileAssignment;
		this.jobClass = jobClass;
		this.context = context;
	}
	
	public void run() {
		
	}
}
