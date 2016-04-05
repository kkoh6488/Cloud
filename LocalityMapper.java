package Cloud;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

// Mapper <inputkey, inputvalue, outputkey, outputvalue>
public class LocalityMapper extends Mapper<Object, Text, LocalityKey, IntWritable> {
		private PlaceJoiner pJoiner;
		private LocalityKey localeKey;

		private static final IntWritable one = new IntWritable(1);

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
	        Configuration conf = context.getConfiguration();
			pJoiner = new PlaceJoiner(conf.get("places-path"));
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
			String tags = dataArray[2];
			String placeId = dataArray[5];
			String neighborhood = null;
			
			String countryName = null; 
			String localityName = null;
			if (pJoiner.IsIdForLocale(placeId))
			{
				String[] data = pJoiner.GetPlaceDataByLocaleID(placeId);
				countryName = data[0];
				localityName = data[1];
			}
			/*
			else
			{
				
			}
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
			context.write(localeKey, one);
			//context.write(placeId, new IntWritable(1));
		}
}
