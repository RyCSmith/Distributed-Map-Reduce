package edu.upenn.cis455.mapreduce.worker;

import java.io.IOException;

import edu.upenn.cis455.httpclient.HttpClient;
import edu.upenn.cis455.mapreduce.worker.WorkerServlet.Status;

public class StatusUpdateThread extends Thread {
	private boolean report = true;
	WorkerServlet servlet;
	String masterLocation;
	
	/**
	 * Default constructor.
	 * @param servlet - The servlet representing the worker node that spawned this thread.
	 * @param masterLocation - Location (IP:Port) of the master node.
	 */
	public StatusUpdateThread(WorkerServlet servlet, String masterLocation) {
		this.servlet = servlet;
		this.masterLocation = masterLocation;
	}
	
	/**
	 * Main operation of the thread. Calls sendUpdate() to send a status 
	 * update regarding this node to the master node every ten seconds.
	 */
	public void run() {
		while (report) {
			sendUpdate();
		}
	}
	
	/**
	 * Asks the thread to stop execution.
	 */
	public void requestStop() {
		report = false;
	}
	
	/**
	 * Sends a status update regarding this node to the master node.
	 */
	public void sendUpdate() {
		System.out.println("StatusUpdateThread: send status " + servlet.status );
		String queryString = "?port=" + servlet.port + "&" + "status=" + servlet.status + "&" + 
				"job=" + servlet.jobClass + "&" + "keysRead=" + servlet.keysRead + "&" + "keysWritten=" + servlet.keysWritten;
		HttpClient client = new HttpClient(masterLocation, queryString);
		try {
			client.makeRequest();
		} catch (IOException e1) {}
		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			System.out.println("ERROR: Error sending status updated. (StatusUpdateThread:51)");
		}
	}
}
