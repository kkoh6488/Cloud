package Cloud;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.FileSplit;

import java.io.File;
import java.io.IOException;
import java.util.Map;

// Mapper <inputkey, inputvalue, outputkey, outputvalue>
public class LocalityMapper extends Mapper<Object, Text, LocalityKey, IntWritable> {
		private PlaceJoiner pJoiner;
		private LocalityKey localeKey;
		private Map<LocalityKey, Integer> localeCounts;

		private static final IntWritable one = new IntWritable(1);

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
	        Configuration conf = context.getConfiguration();
			/*
			File f = new File(conf.get("places-path"));
			if (!f.exists())
			{
				System.err.println("Place.txt not found");
				System.exit(2);
			}
			String filepath = ((FileSplit) context.getInputSplit()).getPath().toString();
			*/
			Path p = new Path(conf.get("places-path"));
			FileSystem fs = FileSystem.get(conf);
			pJoiner = new PlaceJoiner(conf.get("places-path"), fs.open(p));
	    }
		
		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
						
			String[] dataArray = value.toString().split("\t"); //split the data into array
			if (dataArray.length < 6)
			{ 
				//  record with incomplete data
				return; // don't emit anything
			}
			String ownerId = dataArray[1];
			//String tags = dataArray[2];
			String placeId = dataArray[4];
			String neighborhood = "\t \t";
			
			String countryName = "\t \t";
			String localityName = "\t \t";
			String[] data;
			if (pJoiner.IsIdForLocale(placeId))
			{
				data = pJoiner.GetPlaceDataByLocaleID(placeId);
				countryName = data[0];
				localityName = data[1];
			}

			else if (pJoiner.IsIdForKnownNeighborhood(placeId))
			{
				data = pJoiner.GetPlaceDataByNeighborhoodId(placeId);
				countryName = data[0];
				localityName = data[1];
				neighborhood = data[2];
			}
			/*
			// Check tags for neighborhood
			if (tags.length() > 0)
			{
				String[] tagArray = tagString.split(" ");
				for(String tag: tagArray) 
				{
					word.set(tag);
					owner.set(ownerString);
					context.write(word, owner);
				}
			}
			*/
			
			localeKey = new LocalityKey(placeId, countryName, localityName, neighborhood, ownerId);
			if (localeCounts.containsKey(localeKey))
			{
				localeCounts.put(localeKey, localeCounts.get(localeKey) + 1);
			}
			else
			{
				localeCounts.put(localeKey, 1);
			}
			//context.write(localeKey, one);
			//context.write(placeId, new IntWritable(1));
		}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		for (LocalityKey lk : localeCounts.keySet())
		{
			context.write(localeKey, new IntWritable(localeCounts.get(lk)));
		}
	}
}
