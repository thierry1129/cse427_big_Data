package stubs;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;



import org.junit.Before;
import org.junit.Test;

public class Testavg {
	
	Configuration conf = new Configuration();
	
	
	  MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	  ReduceDriver<Text, IntWritable, Text, DoubleWritable> reduceDriver;
	  MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, DoubleWritable> mapReduceDriver;
	  /*
	   * Set up the test. This method will be called before every test.
	   */
	  @Before
	  public void setUp() {
	
	    /*
	     * Set up the mapper test harness.
	     */
	    LetterMapper mapper = new LetterMapper();
	    mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
	    mapDriver.setMapper(mapper);

	    /*
	     * Set up the reducer test harness.
	     */
	    AverageReducer reducer = new AverageReducer();
	    reduceDriver = new ReduceDriver<Text, IntWritable, Text, DoubleWritable>();
	    reduceDriver.setReducer(reducer);

	    /*
	     * Set up the mapper/reducer test harness.
	     */
	    mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, DoubleWritable>();
	    mapReduceDriver.setMapper(mapper);
	    mapReduceDriver.setReducer(reducer);
	  }
	  
	  @Test
	  public void testMapper() {

	    /*
	     * For this test, the mapper's input will be "1 cat cat dog" 
	     * TODO: implement
	     */
		  mapDriver.withInput(new LongWritable(1), new Text("cat meow dog"));
		  /*
		  * The expected output is "cat 1", "cat 1", and "dog 1".
		  */
		  mapDriver.withOutput(new Text("c"), new IntWritable(3));
		  mapDriver.withOutput(new Text("m"), new IntWritable(4));
		  mapDriver.withOutput(new Text("d"), new IntWritable(3));
		  System.out.println("mapper test output ");
		 System.out.println("[c,3], [m,4], [d,3]");
		  /*
		  * Run the test.
		  */
		  mapDriver.runTest();
		  
		  
	  

	  }
	  /*
	   * Test the reducer.
	   */
	  @Test
	  public void testReducer() {

	    /*
	     * For this test, the reducer's input will be "cat 1 1".
	     * The expected output is "cat 2".
	     * TODO: implement
	     */
		  List<IntWritable> values = new ArrayList<IntWritable>();
		  values.add(new IntWritable(3));
		//  values.add(new IntWritable(1));
		  reduceDriver.withInput(new Text("c"), values);
		  /*
		  * The expected output is "cat 2"
		  */
		  reduceDriver.withOutput(new Text("c"), new DoubleWritable(3.0));
		  System.out.println("reducer test output ");
		  System.out.println("[c,3.0]");
		  /*
		  * Run the test.
		  */
		  reduceDriver.runTest();

	  }


	  /*
	   * Test the mapper and reducer working together.
	   */
	  @Test
	  public void testMapReduce() {

	    /*
	     * For this test, the mapper's input will be "1 cat cat dog" 
	     * The expected output (from the reducer) is "cat 2", "dog 1". 
	     * TODO: implement
	     */
		  mapReduceDriver.withInput(new LongWritable(1), new Text("cat meow dog"));
		  /*
		  * The expected output (from the reducer) is "cat 2", "dog 1".
		  */
		  mapReduceDriver.addOutput(new Text("c"), new DoubleWritable(3.0));
		  mapReduceDriver.addOutput(new Text("d"), new DoubleWritable(3.0));  
		  mapReduceDriver.addOutput(new Text("m"), new DoubleWritable(4.0));
		  System.out.println("mapreduce test output ");
		System.out.println("[c,3.0], [d,3.0], [m,4.0] ");
		  /*
		  * Run the test.
		  */
		  mapReduceDriver.runTest();

	  }
	}

	  
	


