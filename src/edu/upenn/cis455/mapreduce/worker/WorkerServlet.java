package edu.upenn.cis455.mapreduce.worker;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

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
	 * Servlet is initialized on container startup and does not wait for first request.
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
		out.println("<body>This is the worker servlet.<br>It can receive jobs "
				+ "via posts to /runmap and /runreduce.</body></html>");
	}
	
	/**
	 * Default handler for POST requests. Distributes the request to the appropriate
	 * method based on path.
	 */
	public void doPost(HttpServletRequest request, HttpServletResponse response) throws java.io.IOException {
		System.out.println("WorkerServlet: Received a connection " + request.getServletPath());
		if (request.getServletPath().equalsIgnoreCase("/runmap")) {
			mapHandler(request, response);
		}
		else if (request.getServletPath().equalsIgnoreCase("/runreduce"))
			reduceHandler(request, response);
		else if (request.getServletPath().equalsIgnoreCase("/pushdata"))
			receiveDataHandler(request, response);
	}		
	
	/**
	 * Handles POST requests for mapping operations.
	 * @param request
	 * @param response
	 * @throws java.io.IOException
	 */
	public void mapHandler(HttpServletRequest request, HttpServletResponse response) throws java.io.IOException {
		long startTime = System.currentTimeMillis();
		//reset statuses
		status = Status.MAPPING;
		resetKeysRead();
		resetKeysWritten();
		makeDirectories(); // creates new spool-in / spool-out directories
		
		//parse params from request
		String jobClass = request.getParameter("job");
		this.jobClass = jobClass;
		int numThreads = Integer.parseInt(request.getParameter("numThreads"));
		String relativeInputDir = request.getParameter("input");
		HashMap<String, String> workerNodeMap = retrieveWorkerNodes(request.getParameterMap());
		
		//send response indicating that request was received
		response.setContentType("text/html");
		PrintWriter out = response.getWriter();
		out.println("RECEIVED");
		out.close();
		
		ArrayList<ArrayList<FileAssignment>> fileAssignments = splitWork(numThreads, getFullDirectoryPath(relativeInputDir));
		MapperContext context = new MapperContext(this, workerNodeMap, getFullDirectoryPath("spool-out"));
		ArrayList<MapperThread> threadsList = new ArrayList<MapperThread>();
		
		//load requested class and create all threads
		for (int i = 0; i < numThreads; i++) {
			//load class
			Job mapperJob = null;
			try {
				Class mapperClass = Class.forName(jobClass);
				mapperJob = (Job) mapperClass.newInstance();
			} catch (Exception e) {
				System.out.println("ERROR: Error while instatiating instance of requested class (WorkerServlet:146");
			}
			threadsList.add(new MapperThread(this, fileAssignments.get(i), mapperJob, context));
		}
		
		//start all threads
		for (MapperThread thread : threadsList) {
			thread.start();
			System.out.println("WorkerServlet: Starting MapperThread: " + thread.getName() + " with assignments:");
			for (FileAssignment assign : thread.fileAssignments) {
				System.out.println("\t" + assign);
			}
		}
		//join all threads
		for (MapperThread thread : threadsList) {
			try {
				thread.join();
			} catch (InterruptedException e) {
				System.out.println("ERROR: MapperThread " + thread.getName() +
						" received an InterruptedException after joining (WorkerServlet:164");
			}
		}	
		//close PrintWriters opened in context
		context.closeWriters();
		//send files to other workers
		pushDataToOtherWorkers(workerNodeMap);
		//update status and force a status update to be sent without waiting for regular 10 second interval
		System.out.println("WorkerServlet: Mapping Complete. Total duration: " + (System.currentTimeMillis() - startTime) / 1000);
		status = Status.WAITING;
		update.sendUpdate();
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
			System.out.println("ERROR: Error counting number of lines in file: " + 
					file.getName() + " (WorkerServlet:286");
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
	 * Creates a fileName for data to be placed in the spool-in directory.
	 * First attempts to create a file with the name reflecting the ip address that sent the data.
	 * If this file already exists, appends a random number to this ip until a unique name
	 * has been found (case = more than one transmission from a worker during single job / two
	 * workers operating on the same machine.)
	 * @param address - ip address of machine sending the data (in String form)
	 * @return
	 */
	public String getNewRandomFileName(String address) {
		Random rand = new Random();
		String spoolInDir = getFullDirectoryPath("spool-in");
		String rootName = spoolInDir + "/" + address;
		String fullName = rootName + "-" + rand.nextInt(1000000000);
		File newFile = new File(fullName);
		while (newFile.exists()) {
			fullName = rootName + "-" + rand.nextInt(1000000000);
			newFile = new File(fullName);
		}
		return fullName + ".txt";
	}
	
	/**
	 * Handles POST requests for reducing operations.
	 * @param request
	 * @param response
	 * @throws java.io.IOException
	 */
	public void reduceHandler(HttpServletRequest request, HttpServletResponse response) throws java.io.IOException {
		long startTime = System.currentTimeMillis();
		status = Status.REDUCING;
		
		//parse params from request
		String jobClass = request.getParameter("job");
		int numThreads = Integer.parseInt(request.getParameter("numThreads"));
		String relativeOutputDir = request.getParameter("output");
		
		//send response indicating that request was received
		response.setContentType("text/html");
		PrintWriter out = response.getWriter();
		out.println("RECEIVED");
		out.close();
		
		File sortedMasterFile = sortAndCreateMasterFile();
		int[] breakPoints = findBreakPoints(sortedMasterFile, numThreads);
		
		ArrayList<FileAssignment> fileAssignments = createReducerFileAssignments(breakPoints, sortedMasterFile);
		ReducerContext context = new ReducerContext(this, getFullDirectoryPath(relativeOutputDir));
		ArrayList<ReducerThread> threadsList = new ArrayList<ReducerThread>();
		
		//load requested class and create all threads
		for (int i = 0; i < numThreads; i++) {
			//load class
			Job reducerJob = null;
			try {
				Class reducerClass = Class.forName(jobClass);
				reducerJob = (Job) reducerClass.newInstance();
			} catch (Exception e) {
				System.out.println("ERROR: Error while instatiating instance of requested class (WorkerServlet:404");
			}
			threadsList.add(new ReducerThread(this, fileAssignments.get(i), reducerJob, context));
		}
		
		//start all threads
		for (ReducerThread thread : threadsList) {
			thread.start();
			System.out.println("WorkerServlet: Starting ReducerThread: " + thread.getName() + " with assignments:");
				System.out.println("\t" + thread.fileAssignment);
		}
		//join all threads
		for (ReducerThread thread : threadsList) {
			try {
				thread.join();
			} catch (InterruptedException e) {
				System.out.println("ERROR: ReducerThread " + thread.getName() +
						" received an InterruptedException after joining (WorkerServlet:420");
			}
		}	
		
		//close PrintWriter opened in context
		context.closeWriter();
		
		//reset statuses and force status update to be sent without waiting for regular 10 second interval
		System.out.println("WorkerServlet: Reducing Complete. Total duration: " + (System.currentTimeMillis() - startTime) / 1000);
		resetKeysRead();
		resetKeysWritten();
		this.jobClass = null;
		status = Status.IDLE;
		update.sendUpdate();
	}
	
	/**
	 * Executes Unix commands as outside processes to concatenate and sort data in the spool-in directory.
	 * Creates a file named sortedMasterFile.txt and saves all data to it.
	 * @return
	 */
	public File sortAndCreateMasterFile() {
		System.out.println("WorkerServlet: Concatenating and sorting \'spool-in\' files as an "
				+ "outside unix process. (This may take some time)");
		String sortedMasterFilePath = null;
		try {
			String fullPath = getFullDirectoryPath("spool-in");
			String allFiles = fullPath + "/*.txt";
			String masterFile = fullPath + "/masterFile.txt";
			String command = "cat " + allFiles + " > " + masterFile;
			String[] commandArray = {"/bin/sh", "-c", command};
			Process proc = Runtime.getRuntime().exec(commandArray);
			proc.waitFor();
			sortedMasterFilePath = fullPath + "/sortedMasterFile.txt";
			String command2 = "sort -t $'\t' " + masterFile + " -o " + sortedMasterFilePath;
			String[] commandArray2 = {"/bin/sh", "-c", command2};
			Process proc2 = Runtime.getRuntime().exec(commandArray2);
			proc2.waitFor();
		} catch (Exception e) {
			System.out.println("ERROR: Error while running outside Unix cat/sort processes (WorkerServlet:455)");
		}
		return new File(sortedMasterFilePath);
	}
	
	/**
	 * Receives a stream of data from another worker node and saves to a file in spool-in directory.
	 * @param request
	 * @param response
	 */
	public void receiveDataHandler(HttpServletRequest request, HttpServletResponse response) {
		try {
			File outputFile = new File(getNewRandomFileName(request.getRemoteAddr()));
			System.out.println("WorkerServlet: Attempting to create file for incoming data: " + outputFile.getAbsolutePath());
			outputFile.createNewFile();
			FileOutputStream writer = new FileOutputStream(outputFile, true);
	
			InputStream in = request.getInputStream();
			byte[] bytes = new byte[20000];
			int bytesRead;

			while ((bytesRead = in.read(bytes)) != -1) {
			    writer.write(bytes, 0, bytesRead);
			}			
		
			response.setContentType("text/plain");
			PrintWriter out = response.getWriter();
			out.println("SUCCESS");
			in.close();
		} catch (IOException e) {
			System.out.println("ERROR: Error receiving data from " + request.getRemoteAddr() + " (WorkerServlet:480)");
		}
		
	}
	
	/**
	 * Pushes each file in the spool-out directory to the corresponding worker node.
	 * @param workerNodeMap
	 */
	public void pushDataToOtherWorkers(HashMap<String, String> workerNodeMap) {
		File spoolOutDir = new File(getFullDirectoryPath("spool-out"));
		List<File> temp = Arrays.asList(spoolOutDir.listFiles());
		for (File file : temp) {
			for (String key : workerNodeMap.keySet()) {
				if (file.getName().contains(key)) {
					System.out.println("WorkerServlet: Pushing file data: " + file.getName() + " to " + key + " at " + workerNodeMap.get(key));
					pushDataToWorker(file, workerNodeMap.get(key));
				}
			}
		}
	}
	
	/**
	 * Streams the contents of the given file to another worker node located
	 * at the provided IP and port.
	 * @param file
	 * @param ipAndPort
	 */
	public void pushDataToWorker(File file, String ipAndPort) {
		try {
			
			String url = "http://" + ipAndPort + "/worker/pushdata";
			URL obj = new URL(url);
			HttpURLConnection client = (HttpURLConnection) obj.openConnection();
			client.setRequestMethod("POST");
			
			client.setDoOutput(true);
			BufferedWriter outStream = new BufferedWriter(new OutputStreamWriter(client.getOutputStream()));
			
			BufferedReader reader = new BufferedReader(new FileReader(file));
			String line;
			while ((line = reader.readLine()) != null) {
				outStream.write(line + "\n");
			}
			
			outStream.flush();
			outStream.close();
	
			BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
			String inputLine;
			StringBuffer response = new StringBuffer();
			while ((inputLine = in.readLine()) != null) {
				response.append(inputLine);
			}
			in.close();
	
		} catch (IOException e) {
			System.out.println("ERROR: Error pushing data to " + ipAndPort + " (WorkerServlet:527)");
		}
	}
	
	/**
	 * Calculates (approximately) even break points within the file while making sure
	 * that all series of entries for a given key will be handled by the same thread.
	 * @param file
	 * @param numThreads
	 * @return
	 */
	public int[] findBreakPoints(File file, int numThreads) {
		if (numThreads == 1)
			return new int[0];
		int[] breakPoints = new int[numThreads];
		int numLines = countNumberLines(file);
		int linesPerThread = numLines / numThreads;
		String lastWord = "";
		int linesRead = 0;
		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));
			String currentLine;
			for (int i = 1; i < numThreads; i++) {
				while ((currentLine = reader.readLine()) != null) {
					String currentWord = currentLine.substring(0, currentLine.indexOf("\t"));
					linesRead++;
					if (linesRead > ((linesPerThread * i) - 50)) {
						if (!lastWord.equalsIgnoreCase(currentWord)) {
							breakPoints[i - 1] = linesRead;
							break;
						}
					}
					lastWord = currentWord;
				}
			}
			reader.close();
		} catch (IOException e) {
			System.out.println("ERROR: Error calculating break points in file (WorkerServlet:557");
		}
		breakPoints[numThreads - 1] = numLines;
		return breakPoints;
	}
	
	/**
	 * Creates FileAssignments for ReducerThreads based on break points and a file.
	 * @param breakPoints
	 * @param sortedMasterFile
	 * @return
	 */
	public ArrayList<FileAssignment> createReducerFileAssignments(int[] breakPoints, File sortedMasterFile) {
		ArrayList<FileAssignment> assignments = new ArrayList<FileAssignment>();
		if (breakPoints.length == 0) {
			assignments.add(new FileAssignment(sortedMasterFile));
			return assignments;
		}
		for (int i = 0; i < breakPoints.length; i++) {
			FileAssignment current;
			if (i == 0)
				current = new FileAssignment(sortedMasterFile, 1, breakPoints[i] - 1);
			else if (i == breakPoints.length - 1)
				current = new FileAssignment(sortedMasterFile, breakPoints[i - 1], breakPoints[i]);
			else
				current = new FileAssignment(sortedMasterFile, breakPoints[i - 1], breakPoints[i] - 1);
			assignments.add(current);
		}
		return assignments;
	}
	
	/**
	 * Increments keysRead value by 1;
	 */
	public void incrementKeysRead() {
		synchronized (keysRead) {
			keysRead += 1;
		}
	}
	
	/**
	 * Increments keysWritten value by 1.
	 */
	public void incrementKeysWritten() {
		synchronized (keysWritten) {
			keysWritten += 1;
		}
	}
	
	/**
	 * Sets keysRead value to 0;
	 */
	public void resetKeysRead() {
		synchronized (keysRead) {
			keysRead = 0;
		}
	}
	
	/**
	 * Sets keysWritten value to 0;
	 */
	public void resetKeysWritten() {
		synchronized (keysWritten) {
			keysWritten = 0;
		}
	}
}
  
