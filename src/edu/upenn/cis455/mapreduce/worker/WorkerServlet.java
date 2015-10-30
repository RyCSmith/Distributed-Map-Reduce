package edu.upenn.cis455.mapreduce.worker;

import java.io.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.*;
import javax.servlet.http.*;

import edu.upenn.cis455.mapreduce.Job;
import edu.upenn.cis455.mapreduce.job.*;

public class WorkerServlet extends HttpServlet {
	public static enum Status { MAPPING, WAITING, REDUCING, IDLE };
	StatusUpdateThread update;
	static final long serialVersionUID = 455555002;
	String port;
	Status status;
	String jobClass;
	Integer keysRead;
	Integer keysWritten;
	String storageRoot;
	
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
		storageRoot = config.getInitParameter("storagedir");
		update.start();
	}
	
	/**
	 * Creates empty local storage directories for the current job.
	 * Deletes old job directories first if present.
	 */
	private void makeDirectories() {
		String spoolInPath;
		String spoolOutPath;
		if (storageRoot.endsWith("/")) {
			spoolInPath = storageRoot + "spool-in";
			spoolOutPath = storageRoot + "spool-out";
		}
		else {
			spoolInPath = storageRoot + "/spool-in";
			spoolOutPath = storageRoot + "/spool-out";
		}
		//empties and deletes directories if they exist, creates new ones
		File spoolInFile = new File(spoolInPath);
		File spoolOutFile = new File(spoolOutPath);
		if (spoolInFile.exists()) {
			File[] files = spoolInFile.listFiles();
			for (File file : files) {
				file.delete();
			}
		    spoolInFile.delete();
		}
		if (spoolOutFile.exists()) {
			File files[] = spoolOutFile.listFiles();
			for (File file : files) {
				file.delete();
			}
		    spoolOutFile.delete();
		}
		spoolInFile.mkdir();
		spoolOutFile.mkdir();		
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
		makeDirectories(); // creates new spool-in / spool-out directories
		String jobClass = request.getParameter("job");
		int numThreads = Integer.parseInt(request.getParameter("numThreads"));
		String relativeInputDir = request.getParameter("input");
		HashMap<String, String> workerNodeMap = retrieveWorkerNodes(request.getParameterMap());
		ArrayList<ArrayList<FileAssignment>> fileAssignments = splitWork(numThreads, getFullDirectoryPath(relativeInputDir));
		WorkerContext context = new WorkerContext(workerNodeMap, getFullDirectoryPath("spool-out"));
		ArrayList<MapperThread> threadsList = new ArrayList<MapperThread>();
		for (int i = 0; i < numThreads; i++) {
			//load class
			Job mapperJob = null;
			try {
				Class mapperClass = Class.forName(jobClass);
				mapperJob = (Job) mapperClass.newInstance();
			} catch (InstantiationException | IllegalAccessException e) {
				e.printStackTrace();
			}catch (ClassNotFoundException e1) {
				e1.printStackTrace();
			}
			threadsList.add(new MapperThread(fileAssignments.get(i), mapperJob, context));
		}
		
		//start all threads
		for (MapperThread thread : threadsList) {
			//thread.start();
			System.out.println("Starting thread: " + thread.getName());
			for (FileAssignment assign : thread.fileAssignments) {
				System.out.println("\t" + assign);
			}
		}
		//join all threads
		for (MapperThread thread : threadsList) {
//			try {
//				//thread.join();
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
		}		
		status = Status.WAITING;
	}
	
	public HashMap<String, String> retrieveWorkerNodes(Map<String, String[]> paramMap) {
		HashMap<String, String> workerMap = new HashMap<String, String>();
		for (String key : paramMap.keySet()) {
			if (key.startsWith("worker"))
				workerMap.put(key, paramMap.get(key)[0]);
		}
		return workerMap;
	}
	
	/**
	 * Reads contents of a directory and splits the work (approximately) 
	 * evenly among the number of threads that will be spawned.
	 * Handles all cases of (numThreads == numFiles, numThread > numFiles, numThreads < numFiles)
	 * @param numThreads
	 * @param inputDir
	 * @return fileAssignments - list containing a list of FileAssignments for each thread.
	 */
	public ArrayList<ArrayList<FileAssignment>> splitWork(int numThreads, String inputDir) {
		//initialize Assignments list
		ArrayList<ArrayList<FileAssignment>> fileAssignments = new ArrayList<ArrayList<FileAssignment>>();
		for (int i = 0; i < numThreads; i++) {
			fileAssignments.add(new ArrayList<FileAssignment>());
		}
		assert (fileAssignments.size() == numThreads);
		
		//check what's in the directory
		File directory = new File(inputDir);
		List<File> temp = Arrays.asList(directory.listFiles());
		ArrayList<File> files = new ArrayList<File>();
		for (File file : temp) {
			if (!file.getName().contains(".DS_Store"))
				files.add(file);
		}
		
		if (files.size() >= numThreads) 
			distributeFiles(numThreads, files, fileAssignments);
		else 
			divideFiles(numThreads, files, fileAssignments);
		return fileAssignments;
		
	}
	
	/**
	 * Divides all the files among the given number of threads (where there are mores files than threads). 
	 * Breaks up the files into sections (starting with largest files) so that all threads are assigned one job.
	 * @param numThreads
	 * @param files
	 * @param fileAssignments
	 */
	private void divideFiles(int numThreads, ArrayList<File> files, ArrayList<ArrayList<FileAssignment>> fileAssignments) {
		Collections.sort(files, new FileLengthComparator());
		int fileCounter = 0;
		int jobsAssigned = 0;
		int threadsPerFile = numThreads / files.size();
		int remainder = numThreads % files.size();
		
		while (fileCounter < remainder) {
			File current = files.get(fileCounter);
			int totalLines = countNumberLines(current);
			int linesPerThread = totalLines / (threadsPerFile + 1);
			for (int i = 0; i < (threadsPerFile + 1); i++) {
				FileAssignment fa;
				if (i == threadsPerFile)
					fa = new FileAssignment(current, (i * linesPerThread) + 1, totalLines);
				else
					fa = new FileAssignment(current, (i * linesPerThread) + 1, (i + 1) * linesPerThread);
				fileAssignments.get(jobsAssigned).add(fa);
				jobsAssigned++;
			}
			fileCounter++;
		}
		while (fileCounter < files.size()) {
			File current = files.get(fileCounter);
			int totalLines = countNumberLines(current);
			int linesPerThread = totalLines / threadsPerFile;
			if (threadsPerFile == 1) { //handles case that not all files have to be divided
				FileAssignment fa = new FileAssignment(current);
				fileAssignments.get(jobsAssigned).add(fa);
				jobsAssigned++;
			}
			else {
				for (int i = 0; i < threadsPerFile; i++) {
					FileAssignment fa;
					if (i == threadsPerFile - 1)
						fa = new FileAssignment(current, (i * linesPerThread) + 1, totalLines);
					else
						fa = new FileAssignment(current, (i * linesPerThread) + 1, (i + 1) * linesPerThread);
					fileAssignments.get(jobsAssigned).add(fa);
					jobsAssigned++;
				}
			}
			fileCounter++;
		}
	}
	
	/**
	 * Counts the number of lines in a file.
	 * @param file
	 * @return
	 */
	private int countNumberLines(File file) {
		int count = 0;
		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));
			String line;
			while ((line = reader.readLine()) != null) {
				count++;
			}
		} catch (Exception e) {
			return 2;
		}
		return count;
	}
	
	/**
	 * Creates FileAssignments and distributes them (approximately) evenly among the threads
	 * that will be created. Each thread deals in full files (numThreads < numFiles).
	 * @param numThreads
	 * @param files
	 * @param fileAssignments
	 */
	private void distributeFiles(int numThreads, ArrayList<File> files, ArrayList<ArrayList<FileAssignment>> fileAssignments) {
		Collections.sort(files, new FileLengthComparator());	
		int currentThread = 0;
		boolean countDown = false;
		while (!files.isEmpty()) {
			File current = files.get(0);
			FileAssignment fAssign = new FileAssignment(current);
			fileAssignments.get(currentThread).add(fAssign);
			files.remove(files.get(0));
			
			if (countDown) {
				if (currentThread == 0) {
					countDown = false;
				} else
					currentThread--;
			} else {
				if (currentThread == fileAssignments.size() - 1) {
					countDown = true;
				} else
					currentThread++;
			}
		}
	}
	
	/**
	 * Converts a relative path to a full path within the rootStorage directory.
	 * @param relativePath
	 * @return
	 */
	public String getFullDirectoryPath(String relativePath) {
		String fullPath;
		if (storageRoot.endsWith("/")) {
			if (relativePath.startsWith("/"))
				fullPath = storageRoot + relativePath.substring(1);
			else
				fullPath = storageRoot + relativePath;
		}
		else {
			if (relativePath.startsWith("/"))
				fullPath = storageRoot + relativePath;
			else
				fullPath = storageRoot + "/" + relativePath;
		}
		return fullPath;
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
  
