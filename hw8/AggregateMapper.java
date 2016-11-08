package stubs;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


/**
 * Mapper's input records are:
 *
 *  <key-as-string><,><value-as-integer>
 *
 * This class, AggregateByKeyMapper, generates all (K,V) pairs.
 * Note that K's can have duplicates. For example it might generate:
 * (K,2) and (K,3).
 *
 *
 * @author Mahmoud Parsian
 *
 */
public class AggregateMapper extends
         Mapper<Object, Text, Text, DoubleWritable> {

   // reuse objects
   private Text K2 = new Text();
   private DoubleWritable V2 = new DoubleWritable();

   @Override
   public void map(Object key, Text value, Context context)
         throws IOException, InterruptedException {

      String valueAsString = value.toString().trim();
      String[] tokens = valueAsString.split(",");
      if (tokens.length != 3) {
         return;
      }

      String url = tokens[0]; //this is the movie id
      double frequency =  Double.parseDouble(tokens[2]);   //this should be the rating 
      K2.set(url);
      V2.set(frequency);
      context.write(K2, V2);
   }
   

}