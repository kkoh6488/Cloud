package Cloud;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

// Mapper <inputkey, inputvalue, outputkey, outputvalue>
public class SumLocaleMapper extends Mapper<Object, Text, SummedPlaceKey, IntWritable> {
    private Map<PlacePair, Integer> localeSum = new HashMap<PlacePair, Integer>();
    private PlacePair placePair;
    private IntWritable count = new IntWritable();

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] dataArray = value.toString().split("\t"); //split the data into array
        if (dataArray.length < 4) {
            //  record with incomplete data
            return; // don't emit anything
        }
        String country = dataArray[0];
        String locale = dataArray[1];
        String neighbourhood = dataArray[2];
        int uniqueCount = Integer.parseInt(dataArray[3]);
        placePair = new PlacePair(country, locale);

        // Get rid of duplicate users for each place - this relies on input being sorted

        // Assume that the first one is the locale with the most unique users for a given country
        // We want to add the number of users per neighbourhood to that sum
        if (!localeSum.containsKey(placePair) && !neighbourhood.equals("#")) {
            localeSum.put(placePair, uniqueCount);
        }

        if (!neighbourhood.equals("#")) {
            count.set(uniqueCount);
            context.write(new SummedPlaceKey(country, locale, neighbourhood, uniqueCount), count);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        int sum;
        for (PlacePair p : localeSum.keySet()) {
            sum = localeSum.get(p);
            count.set(sum);
            context.write(new SummedPlaceKey(p.country, p.locale, "#", sum), count);
        }
    }
}
