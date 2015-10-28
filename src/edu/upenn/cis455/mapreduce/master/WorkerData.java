package edu.upenn.cis455.mapreduce.master;

import java.util.Calendar;

public class WorkerData {
	public static enum Status { MAPPING, WAITING, REDUCING, IDLE, ERROR };
	public Calendar timeReceived;
	public Status status;
	public String job;
	public int keysRead;
	public int keysWritten;
	
	public WorkerData(long timeStamp, String statusString, 
			String job, String keysReadString, String keysWrittenString) {
		this.job = job;
		setKeysRead(keysReadString);
		setKeysWritten(keysWrittenString);
		setTimeReceived(timeStamp);
		setStatus(statusString);
	}
	
	public void setTimeReceived(long timeStamp) {
		timeReceived = Calendar.getInstance();
		timeReceived.setTimeInMillis(timeStamp);
	}
	
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
	
	public void setKeysRead(String keysReadString) {
		try {
			keysRead = Integer.parseInt(keysReadString);
		} catch (Exception e) {
			keysRead = 0;
		}
	}
	
	public void setKeysWritten(String keysWrittenString) {
		try {
			keysWritten = Integer.parseInt(keysWrittenString);
		} catch (Exception e) {
			keysWritten = 0;
		}
	}
}
