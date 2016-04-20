package Cloud;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class LocalityDriver {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 3) {
			System.err.println("Usage: LocalityDriver <input1,input2,etc> <places.txt-path> <out>");
			System.exit(2);
		}

		// Check if output paths are in use already
		FileSystem fs = FileSystem.get(conf);
		Path countTempPath = new Path("tempUserCount");
		Path placeTempPath = new Path("tempPlaceCount");
		Path sortLocalesPath = new Path("tempSortLocale");

		// Job chaining file paths
		Path countTempFiles = new Path("tempUserCount/*");
		Path placeTempFiles = new Path("tempPlaceCount/*");
		Path sortLocalesFiles = new Path("tempSortLocale/*");

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

		// Eliminate duplicates and join with place data
		Job userCountJob = new Job(conf, "User count");
		userCountJob.setJarByClass(LocalityDriver.class);
		userCountJob.setNumReduceTasks(3);

		userCountJob.setMapperClass(UserCountMapper.class);
		userCountJob.setMapperClass(PlaceFileMapper.class);

		// Input file paths. The first argument is comma delimited.
		String[] inputFiles = otherArgs[0].split(",");
		for (String s : inputFiles) {
			MultipleInputs.addInputPath(userCountJob, new Path(s), TextInputFormat.class, UserCountMapper.class);
		}
		MultipleInputs.addInputPath(userCountJob, new Path(otherArgs[1]), TextInputFormat.class, PlaceFileMapper.class);
		TextOutputFormat.setOutputPath(userCountJob, countTempPath);

		userCountJob.setReducerClass(UserCountReducer.class);

		userCountJob.setMapOutputKeyClass(PlaceJoinKey.class);
		userCountJob.setMapOutputValueClass(Text.class);

		userCountJob.setOutputKeyClass(Text.class);
		userCountJob.setOutputValueClass(NullWritable.class);

		userCountJob.waitForCompletion(true);

		// Job 2 - Count the number of unique users per place
		Job job = new Job(conf, "Place");
		job.addCacheFile(new Path(otherArgs[1]).toUri());
		job.setNumReduceTasks(3);
		job.setJarByClass(LocalityDriver.class);
		TextInputFormat.addInputPath(job, countTempFiles);
		TextOutputFormat.setOutputPath(job, placeTempPath);

		job.setMapperClass(CountUniqueMapper.class);
		job.setReducerClass(CountUniqueReducer.class);

		job.setMapOutputKeyClass(LocalityKey.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.waitForCompletion(true);

		// Job 3 - Get the top 10 localities
		Job topLocalesJob = new Job(conf, "Top");
		topLocalesJob.setNumReduceTasks(3);
		topLocalesJob.setJarByClass(TopLocaleMapper.class);
		TextInputFormat.addInputPath(topLocalesJob, placeTempFiles);
		TextOutputFormat.setOutputPath(topLocalesJob, sortLocalesPath);

		topLocalesJob.setMapperClass(TopLocaleMapper.class);
		topLocalesJob.setReducerClass(TopLocaleReducer.class);

		topLocalesJob.setMapOutputKeyClass(TopLocaleKey.class);
		topLocalesJob.setMapOutputValueClass(NullWritable.class);

		topLocalesJob.setOutputKeyClass(Text.class);
		topLocalesJob.setOutputValueClass(NullWritable.class);

		topLocalesJob.waitForCompletion(true);

		// Job 4 - Top Neighbourhood for each locale
		Job topNBJob = new Job(conf, "Top NB");
		topNBJob.setNumReduceTasks(3);
		topNBJob.setJarByClass(LocalityDriver.class);
		TextInputFormat.addInputPath(topNBJob, sortLocalesFiles);
		TextOutputFormat.setOutputPath(topNBJob, finalPath);

		topNBJob.setMapperClass(TopNBMapper.class);
		topNBJob.setReducerClass(TopNBReducer.class);

		topNBJob.setMapOutputKeyClass(TopNBKey.class);
		topNBJob.setMapOutputValueClass(NullWritable.class);

		topNBJob.setOutputKeyClass(Text.class);
		topNBJob.setOutputValueClass(NullWritable.class);

		topNBJob.waitForCompletion(true);

	}
}
