package Cloud;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LocalityReducer extends Reducer<LocalityKey, IntWritable, Text, IntWritable> {

	private Text print = new Text();
	
	@Override
	public void reduce(LocalityKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

		/*
		int count = 0;
		for (IntWritable val: values) {
			count += val.get();
		}
		*/

		//print.set(key.getCountry() + "\t" + key.toString());
		print.set(key.toString());
		//context.write(print, key.getUniqueUsers());
	}
}