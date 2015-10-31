package edu.upenn.cis455.mapreduce.master;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;

public class FeederThread extends Thread {
	private boolean proceed;
	HashMap<String, WorkerData> workersStatus;
	private Queue<JobSubmission> jobQueue;
	JobSubmission currentJob;
	Boolean workersReadyToReduce;
	Boolean workersReadyForNextJob;
	
	/**
	 * Default constructor.
	 * @param workersStatus - HashMap containing status updates received from workers by the master thread.
	 */
	public FeederThread(HashMap<String, WorkerData> workersStatus) {
		this.jobQueue = new LinkedList<JobSubmission>();
		this.workersStatus = workersStatus;
		proceed = true;
		workersReadyToReduce = false;
		workersReadyForNextJob = false;
	}
	
	/**
	 * Main operation of the thread. 
	 * Loops and submits jobs to workers in the order in which they are received.
	 * Waits for workers to complete mapping and asks them to reduce when all are finished.
	 */
	public void run() {
		System.out.println("Feeder thread: started");
		while (proceed) {
			try {
				System.out.println("Feeder thread: Get Job");
				currentJob = getNextJob();
				System.out.println("Feeder thread: Send Map job to workers");
				sendJobToWorkers(0, currentJob.className, currentJob.inputDir, currentJob.numMapThreads);
				System.out.println("Feeder thread: Waiting for workers to complete mapping.");
				waitToProceedToReduce();
				System.out.println("Feeder thread: Send Reduce job to workers");
				sendJobToWorkers(1, currentJob.className, currentJob.outputDir, currentJob.numReduceThreads);
				System.out.println("Feeder thread: Waiting for workers to complete reducing.");
				setWorkersReadyForNextJob(false);
				waitToProceedToNextJob();
				System.out.println("Feeder thread: Complete");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
	}
	
	/**
	 * Asks the thread to stop execution.
	 */
	public synchronized void requestStop() {
		proceed = false;
	}
	
	/**
	 * Checks if the thread should continue execution.
	 * @return
	 */
	public synchronized boolean getProceed() {
		return proceed;
	}
	
	/**
	 * Submits a Map Reduce job to the queue for processing.
	 * @param newJob
	 */
	public synchronized void submitJob(JobSubmission newJob) {
		jobQueue.offer(newJob);
		 notifyAll();
	}
	
	/**
	 * Gets the next job to be processed.
	 * @return
	 * @throws InterruptedException
	 */
	public synchronized JobSubmission getNextJob() throws InterruptedException {
		while (jobQueue.size() == 0) {
			wait();
		}
		return jobQueue.poll();
	}
	
	/**
	 * Makes this thread wait to proceed until all workers have completed mapping.
	 * @throws InterruptedException
	 */
	public synchronized void waitToProceedToReduce() throws InterruptedException {
		while (!workersReadyToReduce) {
			wait();
		}
		workersReadyToReduce = false;
	}
	
	/**
	 * Checks if all threads are in the WAITING state (finished mapping, ready to reduce). 
	 * If so, notifies the thread if it is waiting on the workersReadyToReduce variable.
	 */
	public synchronized void checkThreadsReadyToReduce() {
		synchronized (workersStatus) {
			for (String key : workersStatus.keySet()) {
				if (workersStatus.get(key).status != WorkerData.Status.WAITING)
					return;
			}
		}
		workersReadyToReduce = true;
		notifyAll();
	}

	/**
	 * Sends a POST request to the workers containing information about the current job.
	 * @param mode - 0 for MAP, 1 for REDUCE.
	 * @param job - class name of the job to be run.
	 * @param inputDir - input directory for the job to be run.
	 * @param numThreads - number of threads each worker should launch.
	 * @throws Exception
	 */
	private void sendJobToWorkers(int mode, String job, String directory, String numThreads) throws Exception {
		synchronized (workersStatus) {
			for (String key : workersStatus.keySet()) {
				String url = null; 
				if (mode == 0)
					url = "http://" + key + "/worker/runmap";
				else
					url = "http://" + key + "/worker/runreduce";
				URL obj = new URL(url);
				HttpURLConnection client = (HttpURLConnection) obj.openConnection();
				client.setRequestMethod("POST");	
				
				String params;
				if (mode == 0)
					params = params = "job=" + job + "&input=" + directory + "&numThreads=" + numThreads + getActiveWorkersQueryString();
				else
					params = params = "job=" + job + "&output=" + directory + "&numThreads=" + numThreads;
				
				client.setDoOutput(true);
				DataOutputStream outStream = new DataOutputStream(client.getOutputStream());
				outStream.writeBytes(params);
				outStream.flush();
				outStream.close();
		
				BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
				String inputLine;
				StringBuffer response = new StringBuffer();
				while ((inputLine = in.readLine()) != null) {
					response.append(inputLine);
				}
				in.close();
			}
		}
	}
	
	/**
	 * Generates a query string containing all needed info about active workers.
	 * @return
	 */
	private String getActiveWorkersQueryString() {
		int count = 0;
		StringBuilder builder = new StringBuilder();
		synchronized (workersStatus) {
			for (String key : workersStatus.keySet()) {
				WorkerData data = workersStatus.get(key);
				if (System.currentTimeMillis() - data.timeReceived.getTimeInMillis() < 30000) {
					count++;
					builder.append("&worker" + count + "=" + key);
				}
			}
		}
		builder.insert(0, "&numWorkers=" + count);
		return builder.toString();
	}
	
	
	/**
	 * Makes this thread wait to proceed until all workers are ready to receive another job.
	 * @throws InterruptedException
	 */
	public synchronized void waitToProceedToNextJob() throws InterruptedException {
		while (!workersReadyForNextJob) {
			wait();
		}
	}
	
	/**
	 * Checks if all threads are in the IDLE state (finished reducing or never began, ready to map). 
	 * If so, notifies the thread if it is waiting on the workersReadyForNextJob variable.
	 */
	public synchronized void checkThreadsReadyForNextJob() {
		synchronized (workersStatus) {
			for (String key : workersStatus.keySet()) {
				if (workersStatus.get(key).status != WorkerData.Status.IDLE)
					return;
			}
		}
		workersReadyForNextJob = true;
		notifyAll();
	}
	
	public synchronized void setWorkersReadyForNextJob(boolean ready) {
		workersReadyForNextJob = false;
	}
}
