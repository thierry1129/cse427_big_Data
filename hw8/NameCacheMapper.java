
package stubs;



import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;
public class NameCacheMapper extends Mapper<LongWritable,Text,IntWritable,Text> implements
Configurable
{

	private Configuration configuration;
	Map<Integer, Double> topN = new HashMap<Integer, Double>();
	Map<Integer, String> name = new HashMap<Integer, String>();
	IntWritable keyEmit = new IntWritable();
	Text valEmit = new Text();
	public void map(LongWritable k, Text value, Context context) throws IOException, InterruptedException
	{
		
		for (int key : topN.keySet()){
			
     System.out.println("the key in topn set is "+key);
     
			String secondval = name.get(key);
			
			  System.out.println("the val get from the name set is  "+secondval);
			if (secondval!=null){

				// set the key to be movie id , 
				// set the value to be movie name 
				keyEmit.set(key);
				valEmit.set(secondval);
				context.write(keyEmit, valEmit);
			}
			else{
				
			}
		}
		// process file, emit movie id as key, movie sum rating as value 

		//context.write(keyEmit, valEmit);
	}
	@Override
	public Configuration getConf() {
		
		 return configuration;
	}
	@Override
	public void setConf(Configuration arg0) {

		try {
			File f1 = new File("result.txt");
			File f2 = new File("movie_titles.txt");
			FileReader a = new FileReader (f1);



			FileReader b = new FileReader (f2);
			BufferedReader br = new BufferedReader(a);
			BufferedReader br2 = new BufferedReader(b);



			String line = null;
			String line2 = null;
			while ((line = br.readLine()) != null ) {
				String[] tokens = line.split("\\s+");
				System.out.println("haha!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");

				int movid = Integer.parseInt(tokens[1]);
				System.out.println("the movie id to be put is "+movid);
				

				Double sumRating = Double.parseDouble(tokens[0]);
				

				topN.put(movid, sumRating);

			}


			while ((line2 = br2.readLine()) != null ) {

				String[] tokens = line2.split(",");
				int movid = Integer.parseInt(tokens[0]);

				String moviename = tokens[2];

				name.put(movid,moviename);
			}


		}
		catch(IOException ioe){
			System.err.println("Caught exception while getting cached files: " + StringUtils.stringifyException(ioe));
		}


	}
}