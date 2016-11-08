package stubs;


import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


/**
 * Reducer's input is: (K, List<Integer>)
 *
 * Aggregate on the list of values and create a single (K,V), 
 * where V is the sum of all values passed in the list.
 *
 * This class, AggregateByKeyReducer, accepts (K, [2, 3]) and 
 * emits (K, 5).
 *
 *
 * @author Mahmoud Parsian
 *
 */
public class NameCacheReducer  extends
    Reducer<IntWritable, Text, IntWritable, Text> {

      @Override
      public void reduce(IntWritable key, Iterable<Text> values, Context context) 
         throws IOException, InterruptedException {
         
         double sum = 0.0;
         for (Text value : values) {
               sum += value.get();
         }

         context.write(key, new DoubleWritable(sum));
      }
}