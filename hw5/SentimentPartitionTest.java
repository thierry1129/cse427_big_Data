package stubs;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.junit.Test;

public class SentimentPartitionTest {

	SentimentPartitioner<Text, IntWritable> mpart;

	@Test
	public void testSentimentPartition() {

		spart=new SentimentPartitioner<Text, IntWritable>();
		spart.setConf(new Configuration());
		int result;		
		
		/*
		 * Test the words "love", "deadly", and "zodiac". 
		 * The expected outcomes should be 0, 1, and 2. 
		 */
        
 		/*
		 * TODO implement
		 */          
		
	}

}
