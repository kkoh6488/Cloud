package Cloud;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.HashMap;
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
			//Path p = new Path(conf.get("places-path"));
			Path p = new Path(context.getCacheFiles()[0]);
			FileSystem fs = FileSystem.get(conf);
			pJoiner = new PlaceJoiner(fs.open(p));
			localeCounts = new HashMap<LocalityKey, Integer>();
	    }
		
		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
						
			String[] dataArray = value.toString().split("\t"); //split the data into array
			if (dataArray.length < 2)
			{ 
				//  record with incomplete data
				return; // don't emit anything
			}
			//String ownerId = dataArray[1];
			//String tags = dataArray[2];
			String placeId = dataArray[0];
			Integer userCount = Integer.parseInt(dataArray[1]);
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
			else {
				// Not a locale or neighborhood -> Skip this row
				return;
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
			
			localeKey = new LocalityKey(placeId, countryName, localityName, neighborhood);
			/*
			if (localeCounts.containsKey(localeKey))
			{
				localeCounts.put(localeKey, localeCounts.get(localeKey) + 1);
			}
			else
			{
				localeCounts.put(localeKey, 1);
			}
			*/
			context.write(localeKey, new IntWritable(userCount));
			//context.write(localeKey, one);
			//context.write(placeId, new IntWritable(1));
		}

	/*
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		for (LocalityKey lk : localeCounts.keySet())
		{
			context.write(lk, new IntWritable(localeCounts.get(lk)));
		}
	}
	*/
}
