package edu.upenn.cis455.mapreduce.worker;

import java.io.File;

public class FileAssignment {
	File file;
	public Integer startLine;
	public Integer endLine;
	public boolean entireFile;
	
	/**
	 * Constructor used to assign an entire file.
	 * @param fileName
	 */
	public FileAssignment(File file) {
		this.file = file;
		startLine = null;
		endLine = null;
		entireFile = true;
	}
	
	/**
	 * Constructor used to assign a portion of a file.
	 * @param fileName
	 * @param startLine
	 * @param endLine
	 */
	public FileAssignment(File file, int startLine, int endLine) {
		this.file = file;
		this.startLine = startLine;
		this.endLine = endLine;
		entireFile = false;
	}
	
	@Override
	public String toString(){
		if (entireFile)
			return file.getName() + ": FULL";
		else
			return file.getName() + ": start=" + startLine + " end=" + endLine;
	}
}
