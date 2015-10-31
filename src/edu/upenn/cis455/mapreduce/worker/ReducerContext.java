package edu.upenn.cis455.mapreduce.worker;

import edu.upenn.cis455.mapreduce.Context;

public class ReducerContext implements Context {

	String outputDir;
	
	public ReducerContext(String outputDir) {
		this.outputDir = outputDir;
		System.out.println("Reducer Context: context created");
	}
	
	@Override
	public synchronized void write(String key, String value) {
		// TODO Auto-generated method stub
		
	}

}
