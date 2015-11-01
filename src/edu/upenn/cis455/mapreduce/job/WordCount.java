package edu.upenn.cis455.mapreduce.job;

import java.util.HashMap;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Job;

public class WordCount implements Job {

	/**
	 * Map function for WordCount MapReduce job.
	 */
	  public void map(String key, String value, Context context)
	  {
	    HashMap<String, Integer> wordCount = new HashMap<String, Integer>();
	    String[] words = value.split("\\s+");
	    for (String word : words) {
	    	if (wordCount.get(word) == null)
	    		wordCount.put(word, 0);
	    	wordCount.put(word, wordCount.get(word) + 1);
	    }
	    for (String wordKey : wordCount.keySet()) {
	    	context.write(wordKey, wordCount.get(wordKey).toString());
	    }
	  }
  
  	/**
	 * Reduce function for WordCount MapReduce job.
	 */
	  public void reduce(String key, String[] values, Context context)
	  {
	    Integer totalValue = 0;
	    for (String singleVal : values) {
	    	int parsedValue = Integer.parseInt(singleVal);
	    	totalValue += parsedValue;
	    }
	    context.write(key, totalValue.toString());
	  }
  
}
