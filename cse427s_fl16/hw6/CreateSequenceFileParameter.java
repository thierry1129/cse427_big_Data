package stubs;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.fs.Path;


public class CreateSequenceFileParameter extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {

    if (args.length != 2) {
      System.out.printf("Usage: CreateSequenceFile <input dir> <output dir>\n");
      return -1;
    }

    Job job = new Job(getConf());
    job.setJarByClass(CreateSequenceFileParameter.class);
    job.setJobName("Create Sequence File");

    /*
     * TODO implement
     */
    job.setNumReduceTasks(0);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));
    
//    FileOutputFormat.setCompressOutput(job,true);
   // FileOutputFormat.setOutputCompressorClass(job,SnappyCodec.class);
    
  //  SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
    
    
    
    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new Configuration(), new CreateSequenceFileParameter(), args);
    System.exit(exitCode);
  }
}
