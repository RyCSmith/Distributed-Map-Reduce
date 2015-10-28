package edu.upenn.cis455.mapreduce.worker;

import java.io.*;
import java.util.Map;

import javax.servlet.*;
import javax.servlet.http.*;

public class WorkerServlet extends HttpServlet {
	public static enum Status { MAPPING, WAITING, REDUCING, IDLE };
	StatusUpdateThread update;
	static final long serialVersionUID = 455555002;
	String port;
	Status status;
	String jobClass;
	Integer keysRead;
	Integer keysWritten;
	
	/**
	 * Servlet is initialized on container startup and doeds not wait for first request.
	 * Starts a status updates thread.
	 */
	@Override
	public void init(ServletConfig config) {
		port = config.getInitParameter("port");
		status = Status.IDLE;
		jobClass = null;
		keysRead = 0;
		keysWritten = 0;
		update = new StatusUpdateThread(this, config.getInitParameter("master"));
		update.start();
	}

	public void doGet(HttpServletRequest request, HttpServletResponse response) throws java.io.IOException {
		response.setContentType("text/html");
		PrintWriter out = response.getWriter();
		out.println("<html><head><title>Worker</title></head>");
		out.println("<body>Hi, I am the worker!</body></html>");
	}
	
	/**
	 * Default handler for POST requests. Distributes the request to the appropriate
	 * method based on path.
	 */
	public void doPost(HttpServletRequest request, HttpServletResponse response) throws java.io.IOException {
		if (request.getServletPath().equalsIgnoreCase("/runmap"))
			mapHandler(request, response);
		else if (request.getServletPath().equalsIgnoreCase("/runreduce"))
			reduceHandler(request, response);
	}
	
	/**
	 * Handles POST requests for mapping operations.
	 * @param request
	 * @param response
	 * @throws java.io.IOException
	 */
	public void mapHandler(HttpServletRequest request, HttpServletResponse response) throws java.io.IOException {
		status = Status.MAPPING;
		
		Map<String, String[]> param = request.getParameterMap();
		for (String key : param.keySet()) {
			System.out.println(key + " : " + param.get(key)[0]);
		}
		System.out.println("Worker servlet: status= " + status);
		//do all map stuff, update counts (may need to be zeroed out at start), when complete status changes to waiting
		
		status = Status.WAITING;
	}
	
	/**
	 * Handles POST requests for reducing operations.
	 * @param request
	 * @param response
	 * @throws java.io.IOException
	 */
	public void reduceHandler(HttpServletRequest request, HttpServletResponse response) throws java.io.IOException {
		System.out.println("GOT A REDUCE CALL");
		status = Status.REDUCING;
		
		//do all reduce stuff, update counts (may need to be zeroed out at start) when complete status changes to IDLE
		
		status = Status.IDLE;
	}
	
	
}
  
