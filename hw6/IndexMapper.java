package stubs;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

public class IndexMapper extends Mapper<Text, Text, Text, Text> {

	@Override
	public void map(Text key, Text value, Context context) throws IOException,
	InterruptedException {
		FileSplit fsplit = (FileSplit) context.getInputSplit();
		Path path = fsplit.getPath();
		String fileName = path.getName();
		String line = value.toString();
		StringBuffer b = new StringBuffer();
		b.append(fileName);
		b.append('@');

		b.append(key);
		Text t = new 	Text(b.toString());
		for (String word : line.split("\\W+"))
		{
			if (word.length() > 0)
			{
				context.write(new Text(word), t); 
			}
		}

	
	}
}