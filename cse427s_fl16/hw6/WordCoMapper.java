package stubs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCoMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {


		String line = value.toString();


		StringBuffer b = new StringBuffer();
		boolean bol = true;
		for(String word : line.split("\\W+"))
		{
			if (word.length() > 0)
			{


				if(bol)
				{
					b.append(word.toLowerCase());
					bol = false;
					continue;
				}
				b.append(',');
				b.append(word.toLowerCase());
				context.write(new Text(b.toString()), new IntWritable(1));
				b.delete(0, b.length());
				b.append(word.toLowerCase());
			}
		}	    
	}
}



