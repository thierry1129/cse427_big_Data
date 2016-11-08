package stubs;

import org.apache.log4j.Logger;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;


//import org.dataalgorithms.util.HadoopUtil;

/**
 * TopNDriver: assumes that all K's are unique for all given (K,V) values.
 * Uniqueness of keys can be achieved by using AggregateByKeyDriver job.
 *
 * @author Mahmoud Parsian
 *
 */
public class NameCacheDriver  extends Configured implements Tool {

   private static Logger THE_LOGGER = Logger.getLogger(NameCacheDriver.class);

   public int run(String[] args) throws Exception {
      Job job = new Job(getConf());

      job.setJarByClass(NameCacheDriver.class);
    
      job.setJobName("getMovieName");



      job.setMapperClass(NameCacheMapper.class);
     // job.setReducerClass(NameCacheReducer.class);
      job.setNumReduceTasks(0);


      job.setMapOutputKeyClass(IntWritable.class);   
      job.setMapOutputValueClass(Text.class);   
      
  
      job.setOutputKeyClass(IntWritable.class);
     job.setOutputValueClass(Text.class);


      FileInputFormat.setInputPaths(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      boolean status = job.waitForCompletion(true);
      THE_LOGGER.info("run(): status="+status);
      return status ? 0 : 1;
   }

   /**
   * The main driver for "Top N" program.
   * Invoke this method to submit the map/reduce job.
   * @throws Exception When there is communication problems with the job tracker.
   */
   public static void main(String[] args) throws Exception {


      int returnStatus = ToolRunner.run(new Configuration(),new NameCacheDriver(), args);
      System.exit(returnStatus);
   }

}