package edu.upenn.cis455.mapreduce.master;

/**
 * Data structure to hold information about a job submitted
 * to the master node via the web form.
 * This is placed in Queue for processing.
 */
public class JobSubmission {
	public String className;
	public String inputDir;
	public String outputDir;
	public String numMapThreads;
	public String numReduceThreads;
	
	/**
	 * Default constructor.
	 * @param className
	 * @param inputDir
	 * @param outputDir
	 * @param numMapThreads
	 * @param numReduceThreads
	 */
	public JobSubmission(String className, String inputDir, 
			String outputDir, String numMapThreads, String numReduceThreads) {
		this.className = className;
		this.inputDir = inputDir;
		this.outputDir = outputDir;
		this.numMapThreads = numMapThreads;
		this.numReduceThreads = numReduceThreads;
	}
}
