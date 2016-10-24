package stubs;

import java.io.File;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;

public class SequenceFileWriter
{
	public static String findip(String line){


		String IPADDRESS_PATTERN = 
				"(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)";

		Pattern pattern = Pattern.compile(IPADDRESS_PATTERN);
		Matcher matcher = pattern.matcher(line);
		if (matcher.find()) {
			return matcher.group();
		} else{
			return "0.0.0.0";
		}

	}
	public static void main(String[] args) throws IOException {
		String uri = args[1];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(uri);
		Text key = new Text();
		Text value = new Text();
		File infile = new File(args[0]);
		SequenceFile.Writer writer = null;
		try 
		{
			writer = SequenceFile.createWriter(conf, Writer.file(path), 
					Writer.keyClass(key.getClass()),
					Writer.valueClass(value.getClass()), 
					Writer.bufferSize(fs.getConf().getInt("io.file.buffer.size",4096)), 
					Writer.compression(SequenceFile.CompressionType.BLOCK, new DefaultCodec()),
					Writer.metadata(new Metadata()));			

			for (String line : FileUtils.readLines(infile))
			{

				Text key2 = new Text(findip(line)); 

				value.set(line);

				writer.append(key2, value);
			}
		} finally {
			IOUtils.closeStream(writer);
		}			
	}
}