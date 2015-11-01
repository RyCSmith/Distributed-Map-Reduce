package edu.upenn.cis455.mapreduce.master;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import javax.servlet.*;
import javax.servlet.http.*;

import edu.upenn.cis455.mapreduce.worker.StatusUpdateThread;
import edu.upenn.cis455.mapreduce.worker.WorkerServlet.Status;

public class MasterServlet extends HttpServlet {
	static final long serialVersionUID = 455555001;
	Queue<String> flashMessages = new LinkedList<String>();
	HashMap<String, WorkerData> workersStatus;
	FeederThread feederThread;
	
	/**
	 * Starts a thread that manages a queue for job submission.
	 */
	@Override
	public void init(ServletConfig config) {
		workersStatus = new HashMap<String, WorkerData>();
		feederThread = new FeederThread(workersStatus);
		feederThread.start();
	}
	
	/**
	 * Catch all for GET requests. Distributes the request to the appropriate
	 * method based on path.
	 */
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws java.io.IOException {
		if (request.getServletPath().equalsIgnoreCase("/status"))
			handleStatusPage(request, response);
		else 
			acceptWorkerStatus(request, response);
	}
  
	/**
	 * Handles basic GET requests for /status then constructs and displays the Status Page.
	 * @param request
	 * @param response
	 * @throws java.io.IOException
	 */
	public void handleStatusPage(HttpServletRequest request, HttpServletResponse response) throws java.io.IOException {
		//lists worker statuses
		StringBuilder htmlBuilder = new StringBuilder("<html><head><title>MapReduce Status</title></head><body>Ryan Smith<br>rysmit<center>");
		htmlBuilder.append("<h1>Worker Node Statuses</h1><br>");	
		htmlBuilder.append("<table style=\"width:100%;border-style: solid;border-width: 1px;\"><tr><td><u>" +
		"IP:Port</u></td><td><u>Status</u></td><td><u>Job</u></td><td><u>KeysRead</u></td><td><u>KeysWritten</u></td></tr>");
		synchronized (workersStatus) {
			for (String key : workersStatus.keySet()) {
				WorkerData data = workersStatus.get(key);
				if (System.currentTimeMillis() - data.timeReceived.getTimeInMillis() < 30000) {
					htmlBuilder.append("<tr>");
					htmlBuilder.append("<td>" + key + "</td>");
					htmlBuilder.append("<td>" + data.status + "</td>");
					htmlBuilder.append("<td>" + data.job + "</td>");
					htmlBuilder.append("<td>" + data.keysRead + "</td>");
					htmlBuilder.append("<td>" + data.keysWritten + "</td>");
					htmlBuilder.append("</tr>");
				}
			}
		}
		//adds all flash messages above form
		htmlBuilder.append("</table><br><br><br>");
		while (!flashMessages.isEmpty()) {
			htmlBuilder.append(flashMessages.poll());
		}
		//displays job submit form
		htmlBuilder.append(
				"		<h1>Submit a new job:</h1><br><div>" +
				"			<form action=\"/master/status\" method=\"post\">" +
				"			  Class Name:<br>" +
				"			  <input type=\"text\" name=\"className\" style=\"width:300px;\" placeholder=\"e.g., edu.upenn.cis.cis455.mapreduce.job.MyJob\"><br>" +
				"			  Input Directory:<br>" +
				"			  <input type=\"text\" name=\"inputDir\" style=\"width:300px;\" placeholder=\"(relative)\"><br>" +
				"			  <br>" +
				"			  Output Directory:<br>" +
				"			  <input type=\"text\" name=\"outputDir\" style=\"width:300px;\" placeholder=\"(relative)\"><br>" +
				"			  <br>" +
				"			  Map Threads per Worker:<br>" +
				"			  <input type=\"text\" name=\"numMapThreads\" style=\"width:300px;\"><br>" +
				"			  <br>" +
				"			  Reduce Threads per Worker:<br>" +
				"			  <input type=\"text\" name=\"numRedThreads\" style=\"width:300px;\"><br>" +
				"			  <br>" +
				"			  <input type=\"submit\" value=\"Submit\" style=\"width:100px;\">" +
				"			</form>" +
				"		</div>");
		htmlBuilder.append("</center></body></html>");
	    response.setContentType("text/html");
	    PrintWriter out = response.getWriter();
	    out.println(htmlBuilder.toString());
	}
  
	/**
	 * Accepts GET requests containing status updates from worker nodes.
	 * Updates worker status map and informs feederThread that a change has been made and it should
	 * check to see if it is possible to proceed if it is waiting.
	 * @param request
	 * @param response
	 * @throws java.io.IOException
	 */
	public void acceptWorkerStatus(HttpServletRequest request, HttpServletResponse response) throws java.io.IOException {
		response.setContentType("text/plain");
	    PrintWriter out = response.getWriter();	
	    try {
	    	Map<String, String[]> param = request.getParameterMap();
	    	String port = param.get("port")[0];
	    	String status = param.get("status")[0];
	    	String job = param.get("job")[0];
	    	String keysRead = param.get("keysRead")[0];
	    	String keysWritten = param.get("keysWritten")[0];
	    	WorkerData data = new WorkerData(System.currentTimeMillis(), status, job, keysRead, keysWritten);
	    	synchronized (workersStatus) {
	    		workersStatus.put(request.getRemoteAddr() + ":" + port, data);
	    	}
	    	feederThread.checkThreadsReadyToReduce();
	    	feederThread.checkThreadsReadyForNextJob();
	    	out.println("SUCCESS");
	    } catch (Exception e) {
	    	System.out.println("ERROR: Error receiving status from worker " 
	    			+ request.getRemoteAddr() + " (MasterServlet:128)");
	    }     
	}
	
	/**
	 * Handles requests submitted via the POST form. 
	 * Creates and submit a new JobSubmission for processing if input fits proper format.
	 * Redirects back to the status page.
	 */
	public void doPost(HttpServletRequest request, HttpServletResponse response) throws java.io.IOException {
		try {
	    	Map<String, String[]> param = request.getParameterMap();
	    	String className = param.get("className")[0];
	    	String inputDir = param.get("inputDir")[0];
	    	String outputDir = param.get("outputDir")[0];
	    	String numMapThreads = param.get("numMapThreads")[0];
	    	String numRedThreads = param.get("numRedThreads")[0];
	    	if (checkFormData(className, inputDir, outputDir, numMapThreads, numRedThreads)) {
	    		feederThread.submitJob(new JobSubmission(className, inputDir, outputDir, numMapThreads, numRedThreads));
	    	}
	    } catch (Exception e) {
	    	flashMessages.add("<div style=\"color:red;\">An error was encountered while receiving your submission. Please try again</div>");
	    	response.sendRedirect("/master/status");
	    	System.out.println("ERROR: Error receiving job request from " + request.getRemoteAddr() + " (MasterServlet:153)");
	    }     
		response.sendRedirect("/master/status");
	}	
	
	/**
	 * Checks that data received in POST form is of correct format. 
	 * Adds messages to flash message queue if not.
	 * @param className
	 * @param inputDir
	 * @param outputDir
	 * @param numMapThreads
	 * @param numRedThreads
	 * @return valid - boolean indicating whether all fields contained valid input.
	 */
	private boolean checkFormData(String className, String inputDir, 
			String outputDir, String numMapThreads, String numRedThreads) {
		boolean valid = true;
		if (className.equals("") || className == null) {
			flashMessages.add("<div style=\"color:red;\">Job Name was not included!</div>");
			valid = false;
		}
		if (inputDir.equals("") || inputDir == null) {
			flashMessages.add("<div style=\"color:red;\">Input directory was not included!</div>");
			valid = false;
		}
		if (outputDir.equals("") || outputDir == null) {
			flashMessages.add("<div style=\"color:red;\">Output directory was not included!</div>");
			valid = false;
		}
		if (numMapThreads.equals("") || numMapThreads == null) {
			flashMessages.add("<div style=\"color:red;\">Number of map threads was not included!</div>");
			valid = false;
		}
		else {
			try {
				Integer.parseInt(numMapThreads);
			} catch (Exception e) {
				flashMessages.add("<div style=\"color:red;\">Number of map threads was not a valid number!</div>");
				valid = false;
			}
		}
		if (numRedThreads.equals("") || numRedThreads == null) {
			flashMessages.add("<div style=\"color:red;\">Number of reducer threads was not included!</div>");
			valid = false;
		}
		else {
			try {
				Integer.parseInt(numRedThreads);
			} catch (Exception e) {
				flashMessages.add("<div style=\"color:red;\">Number of reducer threads was not a valid number!</div>");
				valid = false;
			}
		}
		return valid;
	}
}
  
