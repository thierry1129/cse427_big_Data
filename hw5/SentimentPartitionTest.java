package stubs;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.util.StringUtils;
import org.junit.Before;
import org.junit.Test;




public class SentimentPartitionTest {
	
	 MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
	  MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	  ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	 
	@Before
	 public void setUp() {

		WordMapper mapper = new WordMapper();
	    mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
	    mapDriver.setMapper(mapper);

	    /*
	     * Set up the reducer test harness.
	     */
	    SumReducer reducer = new SumReducer();
	    reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>();
	    reduceDriver.setReducer(reducer);


	    /*
	     * Set up the mapper/reducer test harness.
	     */
	    mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable>();
	    mapReduceDriver.setMapper(mapper);
	    mapReduceDriver.setReducer(reducer);
	  }
	@Test
	public void testSentimentPartition() {
		
		
		mapReduceDriver.withInput( new LongWritable(1),new Text("love, deadly, zodiac"));
		  mapReduceDriver.addOutput(new Text("love"), new IntWritable(0));
		  mapReduceDriver.addOutput(new Text("deadly"), new IntWritable(1));
		  mapReduceDriver.addOutput(new Text("zodiac"), new IntWritable(2));
		
				//  mapReduceDriver.addOutput(new Text("love"), new IntWritable(0));
				
			
				  /*
				  * Run the test.
				  */
				  mapReduceDriver.runTest();
		  
		/*
		 * Test the words "love", "deadly", and "zodiac". 
		 * The expected outcomes should be 0, 1, and 2. 
		 */
        
 		/*
		 * TODO implement
		 */          
		
	}

}
