package stubs;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LetterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    /*
     * TODO implement
     */
	  /*
	     * Convert the line, which is received as a Text object,
	     * to a String object.
	     */
	    String line = value.toString();

	    /*
	     * The line.split("\\W+") call uses regular expressions to split the
	     * line up by non-word characters.
	     * 
	     * If you are not familiar with the use of regular expressions in
	     * Java code, search the web for "Java Regex Tutorial." 
	     */
	    for (String word : line.split("\\W+")) {
	      if (word.length() > 0) {
	        
	        /*
	         * Call the write method on the Context object to emit a key
	         * and a value from the map method.
	         */
	    	  String l = word.substring(0, 1);
	    
	        context.write(new Text(l), new IntWritable(word.length()));
	      }
  }
}}

