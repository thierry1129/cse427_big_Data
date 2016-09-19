package stubs;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {

 
	  int count = 0;
	  int sum = 0;
	  
	
		for (IntWritable value : values) {
		  
	
			sum = sum+ value.get();
			count++;
			
		}
		
		double avg = (double)sum/count;
		context.write(key, new DoubleWritable(avg));
		
		

  }
}