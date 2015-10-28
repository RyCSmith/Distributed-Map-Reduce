package edu.upenn.cis455.mapreduce.master;

public class JobSubmission {
	public String className;
	public String inputDir;
	public String outputDir;
	public String numMapThreads;
	public String numReduceThreads;
	
	public JobSubmission(String className, String inputDir, 
			String outputDir, String numMapThreads, String numReduceThreads) {
		this.className = className;
		this.inputDir = inputDir;
		this.outputDir = outputDir;
		this.numMapThreads = numMapThreads;
		this.numReduceThreads = numReduceThreads;
	}
}
