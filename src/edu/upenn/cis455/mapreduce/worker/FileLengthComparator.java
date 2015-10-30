package edu.upenn.cis455.mapreduce.worker;

import java.io.File;
import java.util.Comparator;

public class FileLengthComparator implements Comparator<File> {
	public int compare(File file1, File file2) {
		long file1length = file1.length();
		long file2length = file2.length();
		if (file1length > file2length)
			return -1;
		if (file1length < file2length)
			return 1;
		else
			return 0;
	}
}
