package Cloud;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

// Mapper <inputkey, inputvalue, outputkey, outputvalue>
public class PlaceMapper extends Mapper<Object, Text, LocalityKey, Text> {
    private PlaceJoiner pJoiner;
    private LocalityKey localeKey;
    private Text output = new Text();

    private static final IntWritable one = new IntWritable(1);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        Path p = new Path(context.getCacheFiles()[0]);
        FileSystem fs = FileSystem.get(conf);
        pJoiner = new PlaceJoiner(fs.open(p));
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
        String placeId = dataArray[0];
        String userId = dataArray[1];
        String neighborhood = "#";

        String countryName;
        String localityName;
        String[] data;
        if (pJoiner.IsIdForLocale(placeId))
        {
            data = pJoiner.GetPlaceDataByLocaleID(placeId);
            countryName = data[0];
            localityName = data[1];
        }

        // If it's a neighbourhood - count the distinct users for the locale
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

        localeKey = new LocalityKey(placeId, countryName, localityName, neighborhood, 1);
        output.set(userId);
        context.write(localeKey, output);

			/*
			localePair = new PlacePair(localityName, countryName);
			if (totalCounts.containsKey(localePair))
			{
				totalCounts.put(localePair, totalCounts.get(localePair) + userCount);
			}
			else
			{
				totalCounts.put(localePair, userCount);
			}
			*/

        //context.write(localeKey, one);
        //context.write(placeId, new IntWritable(1));
    }

	/*
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		for (PlacePair p : totalCounts.keySet())
		{
			context.write(lk, new IntWritable(localeCounts.get(lk)));
		}
	}
	*/
}
