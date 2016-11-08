package stubs;

import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

public class TopNMapper extends
   Mapper<LongWritable, Text, NullWritable, Text> {

   private int N = 10; // default
   private SortedMap<Double, String> top = new TreeMap<Double, String>();
//longwritable, text should be the mapper input 
   @Override
   public void map(LongWritable key, Text value, Context context)
         throws IOException, InterruptedException {
	   
	   
	   
	   
	   
	   
	   
	   
	 //  String line = value.toString();
// this is the movie id 
	   
	   
	//   String[] fields = value.toString().split(" ");
	    
	
	      
	    //then the first field should be the movie id 
	    //  String movieid = fields[0];
	      // then the second field should be the movie sum rating
	      
	      
	//   String sumRate = fields[1];
	   
	   
	   String valueAsString = value.toString().trim();
       String[] tokens = valueAsString.split("\\W+");
       
       
       int movid = Integer.parseInt(tokens[0]);
	  // int movid = (int)movieid;
	//  int movid =  Integer.parseInt(movieid) ;
	  
	 // Double sumRating = Double.parseDouble(sumRate);
	  
	  Double sumRating = Double.parseDouble(tokens[1]);
	  
	  
     // String keyAsString = key.toString();
      
      // get the total amount of rating from the previous mapreduce 
    //  double frequency =  value.get();
      
      
      String compositeValue = movid + "," + sumRating;
      top.put(sumRating, compositeValue);
      // keep only top N
      if (top.size() > N) {
         top.remove(top.firstKey());
      }
   }
   
   @Override
   protected void setup(Context context) throws IOException,
         InterruptedException {
      this.N = context.getConfiguration().getInt("N", 15); // default is top 10
   }
   

   @Override
   protected void cleanup(Context context) throws IOException,
         InterruptedException {
      for (String str : top.values()) {
         context.write(NullWritable.get(), new Text(str));
      }
   }

}