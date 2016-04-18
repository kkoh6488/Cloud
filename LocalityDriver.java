package Cloud;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class LocalityDriver {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 3) {
			System.err.println("Usage: TagDriver <in> <places.txt-path> <out>");
			System.exit(2);
		}

		// Check if output paths are in use already
		FileSystem fs = FileSystem.get(conf);
		Path countTempPath = new Path("tempUserCount");
		Path placeTempPath = new Path("tempPlaceCount");
		Path sortLocalesPath = new Path("tempSortLocale");
		Path finalPath = new Path(otherArgs[2]);
		if (fs.exists(countTempPath)) {
			fs.delete(countTempPath, true);
		}
		if (fs.exists(placeTempPath)) {
			fs.delete(placeTempPath, true);
		}
		if (fs.exists(sortLocalesPath)) {
			fs.delete(sortLocalesPath, true);
		}
		if (fs.exists(finalPath)) {
			fs.delete(finalPath, true);
		}

		// 1. Get rid of duplicates - emit placeID, userID

		// 2. Join - filter only neighbourhoods and locales - no summing, just filter

		// 2. Sum number of unique users per locale - including neighbourhoods
		// Emit locales and neighbourhoods and unique user count

		// 3.

		// Count the unique users per place ID
		Job userCountJob = new Job(conf, "user count");
		userCountJob.setJarByClass(LocalityDriver.class);
		TextInputFormat.addInputPath(userCountJob, new Path(otherArgs[0]));
		TextOutputFormat.setOutputPath(userCountJob, countTempPath);
		userCountJob.setNumReduceTasks(1);

		userCountJob.setMapperClass(UserCountMapper.class);
		userCountJob.setReducerClass(UserCountReducer.class);

		userCountJob.setMapOutputKeyClass(Text.class);
		userCountJob.setMapOutputValueClass(NullWritable.class);

		userCountJob.setOutputKeyClass(Text.class);
		userCountJob.setOutputValueClass(NullWritable.class);

		userCountJob.waitForCompletion(true);

		// Job 2 - Join with place data and filter so only neighborhoods and locales are used.
		Job job = new Job(conf, "place");
		job.addCacheFile(new Path(otherArgs[1]).toUri());
		job.setNumReduceTasks(1); // we use three reducers, you may modify the number
		job.setJarByClass(LocalityDriver.class);
		TextInputFormat.addInputPath(job, countTempPath);
		TextOutputFormat.setOutputPath(job, placeTempPath);

		job.setMapperClass(PlaceMapper.class);
		job.setReducerClass(PlaceReducer.class);

		job.setMapOutputKeyClass(LocalityKey.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.waitForCompletion(true);

		// Job 3 - Sort localities and neighbourhoods to be in decreasing order
		// Only print top neighbourhood for each
		Job sumLocalesJob = new Job(conf, "sum");
		sumLocalesJob.setNumReduceTasks(1);
		sumLocalesJob.setJarByClass(SumLocaleMapper.class);
		TextInputFormat.addInputPath(sumLocalesJob, placeTempPath);
		TextOutputFormat.setOutputPath(sumLocalesJob, sortLocalesPath);

		sumLocalesJob.setMapperClass(SumLocaleMapper.class);
		sumLocalesJob.setReducerClass(SumLocaleReducer.class);

		sumLocalesJob.setMapOutputKeyClass(SummedPlaceKey.class);
		sumLocalesJob.setMapOutputValueClass(IntWritable.class);

		sumLocalesJob.setOutputKeyClass(Text.class);
		sumLocalesJob.setOutputValueClass(NullWritable.class);

		sumLocalesJob.waitForCompletion(true);
		//System.exit(sumLocalesJob.waitForCompletion(true) ? 0 : 1)

		// Job 4 - Top 10 Locales for each Country
		Job topLocalesJob = new Job(conf, "n");
		topLocalesJob.setNumReduceTasks(1);
		topLocalesJob.setJarByClass(LocalityDriver.class);
		TextInputFormat.addInputPath(topLocalesJob, sortLocalesPath);
		TextOutputFormat.setOutputPath(topLocalesJob, finalPath);

		topLocalesJob.setMapperClass(TopLocaleMapper.class);
		topLocalesJob.setReducerClass(TopLocaleReducer.class);

		topLocalesJob.setMapOutputKeyClass(TopLocaleKey.class);
		topLocalesJob.setMapOutputValueClass(IntWritable.class);

		topLocalesJob.setOutputKeyClass(Text.class);
		topLocalesJob.setOutputValueClass(NullWritable.class);

		topLocalesJob.waitForCompletion(true);
	}
}
