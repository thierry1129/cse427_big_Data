package stubs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.util.StringUtils;

public class SentimentPartitioner extends Partitioner<Text, IntWritable> implements
    Configurable {

  private Configuration configuration;
  Set<String> positive = new HashSet<String>();
  Set<String> negative = new HashSet<String>();

  /**
   * Set up the positive and negative hash set in the setConf method.
   */
  @Override
  public void setConf(Configuration configuration) {
     /*
     * Add the positive and negative words to the respective sets using the files 
     * positive-words.txt and negative-words.txt.
     */
    /*
     * TODO implement 
     */ try {
	  File f1 = new File("positive-words.txt");
	  File f2 = new File("negative-words.txt");
	 
	//  FileInputStream fis = new FileInputStream(f1);
	//  FileInputStream fis2 = new FileInputStream(f2);
	 // FileReader a = new FileReader("/home/training/workspace/sentimentanalysis/src/positive-words.txt");
	//  FileReader b = new FileReader("/home/training/workspace/sentimentanalysis/src/negative-words.txt");
	  FileReader a = new FileReader (f1);
	  
	  
	  
	  FileReader b = new FileReader (f2);
	  
	 // FileInputStream fis = new FileInputStream(f1);
	//  FileInputStream fis2 = new FileInputStream(f2);
  
	  BufferedReader br = new BufferedReader(a);
	  BufferedReader br2 = new BufferedReader(b);
	  
	 // BufferedReader br = new BufferedReader(fis);
	 // BufferedReader br2 = new BufferedReader(b);
	  
	  
	  String line = null;
	  String line2 = null;
		while ((line = br.readLine()) != null ) {
			if((line.startsWith(";")==false)){
				
			   positive.add(line);
			 
				}
				else{
					
				}
			   
		   
		}
		while ((line2 = br2.readLine()) != null ) {
			if((line2.startsWith(";")==false)){
				
				 negative.add(line2);
					}
					else{
						
					}
			
			 
			}
	
	 
		br.close();
		br2.close();
	  }
	  catch(IOException ioe){
		  System.err.println("Caught exception while getting cached files: " + StringUtils.stringifyException(ioe));
	  }

	  
  }
  
  
 
  /**
   * Implement the getConf method for the Configurable interface.
   */
  @Override
  public Configuration getConf() {
    return configuration;
  }

  /**
   * You must implement the getPartition method for a partitioner class.
   * This method receives the words as keys (i.e., the output key from the mapper.)
   * It should return an integer representation of the sentiment category
   * (positive, negative, neutral).
   * 
   * For this partitioner to work, the job configuration must have been
   * set so that there are exactly 3 reducers.
   */
  public int getPartition(Text key, IntWritable value, int numReduceTasks) {
    /*
     * TODO implement
     * Change the return 0 statement below to return the number of the sentiment 
     * category; use 0 for positive words, 1 for negative words, and 2 for neutral words. 
     * Use the sets of positive and negative words to find out the sentiment.
     *
     * Hint: use positive.contains(key.toString()) and negative.contains(key.toString())
     * If a word appears in both lists assume it is positive. That is, once you found 
     * that a word is in the positive list you do not need to check if it is in the 
     * negative list. 
     */
	  
	  if (positive.contains(key.toString())){
		  return 0;
		  
		  
	  }
	  else if (negative.contains(key.toString())){
		  return 1;
		  
	  }
	  else{
		  return 2;
		  
	  }
	  
	
  }
}
