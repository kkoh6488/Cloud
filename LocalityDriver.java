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

		Path path = new Path(args[1]);

		// Check if output paths are in use already
		FileSystem fs = FileSystem.get(conf);
		Path countTempPath = new Path("tempUserCount");
		Path sortOutputPath = new Path("tempPlaceCounts");
		Path sumLocalesPath = new Path(otherArgs[2]);
		if (fs.exists(countTempPath)) {
			fs.delete(countTempPath, true);
		}
		if (fs.exists(sortOutputPath)) {
			fs.delete(sortOutputPath, true);
		}
		if (fs.exists(sumLocalesPath)) {
			fs.delete(sumLocalesPath, true);
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

		userCountJob.setMapOutputKeyClass(UserPlaceKey.class);
		userCountJob.setMapOutputValueClass(NullWritable.class);

		userCountJob.setOutputKeyClass(Text.class);
		userCountJob.setOutputValueClass(Text.class);

		userCountJob.waitForCompletion(true);

		// Job 2 - Join with place data and filter so only neighborhoods and locales are used.
		Job job = new Job(conf, "place");
		job.addCacheFile(new Path(otherArgs[1]).toUri());
		job.setNumReduceTasks(1); // we use three reducers, you may modify the number
		job.setJarByClass(LocalityDriver.class);
		TextInputFormat.addInputPath(job, countTempPath);
		TextOutputFormat.setOutputPath(job, sortOutputPath);

		job.setMapperClass(PlaceMapper.class);
		job.setReducerClass(PlaceReducer.class);

		job.setMapOutputKeyClass(LocalityKey.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.waitForCompletion(true);

		// Job 3 - Sort localities and neighbourhoods to be in decreasing order
		Job sumLocalesJob = new Job(conf, "sum");
		sumLocalesJob.setNumReduceTasks(3);
		sumLocalesJob.setJarByClass(SumLocaleMapper.class);
		TextInputFormat.addInputPath(sumLocalesJob, sortOutputPath);
		TextOutputFormat.setOutputPath(sumLocalesJob, sumLocalesPath);

		sumLocalesJob.setMapperClass(SumLocaleMapper.class);
		sumLocalesJob.setReducerClass(SumLocaleReducer.class);

		sumLocalesJob.setMapOutputKeyClass(SummedPlaceKey.class);
		sumLocalesJob.setMapOutputValueClass(IntWritable.class);

		sumLocalesJob.setOutputKeyClass(Text.class);
		sumLocalesJob.setOutputValueClass(NullWritable.class);

		sumLocalesJob.waitForCompletion(true);
		//System.exit(sumLocalesJob.waitForCompletion(true) ? 0 : 1)
	}
}
