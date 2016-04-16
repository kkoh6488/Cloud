package Cloud;

import java.io.File;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

		//conf.addResource(new Path(args[1]));
		Path path = new Path(args[1]);
		//FileSystem fs = path.getFileSystem(conf);
		/*
		File f = new File(otherArgs[1]);
		if (!f.exists())
		{
			System.err.println("Place.txt not found");
			System.exit(2);
		}
		PlaceJoiner pj = new PlaceJoiner(otherArgs[1]);
		System.out.println(pj.IsIdForLocale("xbxI9VGYA5oZH8tLJA"));
		System.out.println(pj.GetLocaleCount());
		for (int i = 0; i < 10; i++) {
			//System.out.println(pj.GetLocales().);
		}
		System.exit(2);
		*/
		// Set the path for places.txt
		//conf.set("places-path", otherArgs[1]);

		// Count the unique users per place ID
		Job userCountJob = new Job(conf, "user count");
		userCountJob.setJarByClass(LocalityDriver.class);
		TextInputFormat.addInputPath(userCountJob, new Path(otherArgs[0]));
		TextOutputFormat.setOutputPath(userCountJob, new Path(otherArgs[2]));
		userCountJob.setNumReduceTasks(1);

		userCountJob.setMapperClass(UserCountMapper.class);
		userCountJob.setReducerClass(UserCountReducer.class);

		userCountJob.setMapOutputKeyClass(Text.class);
		userCountJob.setMapOutputValueClass(IntWritable.class);

		userCountJob.setOutputKeyClass(Text.class);
		userCountJob.setOutputValueClass(IntWritable.class);

		userCountJob.waitForCompletion(true);

		/*
		// Job 2
		Job job = new Job(conf, "place");
		job.addCacheFile(new Path(otherArgs[1]).toUri());
		job.setNumReduceTasks(1); // we use three reducers, you may modify the number
		job.setJarByClass(LocalityDriver.class);
		TextInputFormat.addInputPath(job, new Path("userCountTemp"));
		TextOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

		job.setMapperClass(LocalityMapper.class);
		job.setReducerClass(LocalityReducer.class);
		
		job.setMapOutputKeyClass(LocalityKey.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		*/
	}
}
