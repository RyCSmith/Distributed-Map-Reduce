package edu.upenn.cis455.mapreduce.master;

import java.util.Calendar;

/**
 * Data structure to hold status data received by a worker node.
 */
public class WorkerData {
	public static enum Status { MAPPING, WAITING, REDUCING, IDLE, ERROR };
	public Calendar timeReceived;
	public Status status;
	public String job;
	public int keysRead;
	public int keysWritten;
	
	/**
	 * Default constructor.
	 * @param timeStamp
	 * @param statusString
	 * @param job
	 * @param keysReadString
	 * @param keysWrittenString
	 */
	public WorkerData(long timeStamp, String statusString, 
			String job, String keysReadString, String keysWrittenString) {
		this.job = job;
		setKeysRead(keysReadString);
		setKeysWritten(keysWrittenString);
		setTimeReceived(timeStamp);
		setStatus(statusString);
	}
	
	/**
	 * Sets the time received for this WorkerData to the current time.
	 * @param timeStamp
	 */
	public void setTimeReceived(long timeStamp) {
		timeReceived = Calendar.getInstance();
		timeReceived.setTimeInMillis(timeStamp);
	}
	
	/**
	 * Takes the status (in String form) reported by the worker and assigns
	 * the appropriate enum to represent it.
	 * @param statusString
	 */
	public void setStatus(String statusString) {
		statusString = statusString.trim();
		if (statusString.equalsIgnoreCase("mapping"))
			status = Status.MAPPING;
		else if (statusString.equalsIgnoreCase("waiting"))
			status = Status.WAITING;
		else if (statusString.equalsIgnoreCase("reducing"))
			status = Status.REDUCING;
		else if (statusString.equalsIgnoreCase("idle"))
			status = Status.IDLE;
		else
			status = Status.ERROR;
	}
	
	/**
	 * Takes number of keys read as a String reported by the worker and
	 * sets keysRead variable as an int.
	 * @param keysReadString
	 */
	public void setKeysRead(String keysReadString) {
		try {
			keysRead = Integer.parseInt(keysReadString);
		} catch (Exception e) {
			keysRead = 0;
		}
	}
	
	/**
	 * Takes number of keys written as a String reported by the worker and
	 * sets keysWritten variable as an int.
	 * @param keysWrittenString
	 */
	public void setKeysWritten(String keysWrittenString) {
		try {
			keysWritten = Integer.parseInt(keysWrittenString);
		} catch (Exception e) {
			keysWritten = 0;
		}
	}
}
