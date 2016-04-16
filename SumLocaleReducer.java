package Cloud;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SumLocaleReducer extends Reducer<SummedPlaceKey, IntWritable, Text, Text> {
    HashMap<PlacePair, SummedPlaceKey> topNeighbourhoods = new HashMap<PlacePair, SummedPlaceKey>();
    HashMap<String, Integer> topCounts = new HashMap<String, Integer>();    // Gets the top N per country
    PlacePair tempPair;
    Text empty = new Text();
    Text output = new Text();
    String result;

    @Override
    public void reduce(SummedPlaceKey placeKey, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        String country = placeKey.getCountry();
        String locale = placeKey.getLocale();
        tempPair = new PlacePair(locale, country);

        // If it's a neighbourhood - store the top result per locality
        if (!placeKey.getNeighbourhood().equals("#")) {
            if (!topNeighbourhoods.containsKey(tempPair)) {         // Only get the first one - since they are sorted
                topNeighbourhoods.put(tempPair, placeKey);
            }
        } else {
            // If it's a locale - get the top 10 for each country. Otherwise, skip this locale.
            if (topCounts.containsKey(country)) {
                int numLocs = topCounts.get(country);
                if (numLocs < 10) {
                    topCounts.put(country, numLocs + 1);
                } else {
                    return;
                }
            } else {
                topCounts.put(country, 0);
            }
            if (topNeighbourhoods.containsKey(tempPair)) {
                SummedPlaceKey topNB = topNeighbourhoods.get(tempPair);
                result = country + "\t{(" + locale + ":"
                        + placeKey.getUniqueUsers() + ", " + topNB.getNeighbourhood() + ":" + topNB.getUniqueUsers() + ")};";
            } else {
                result = country + "\t{(" + locale + ":" + placeKey.getUniqueUsers()+ ")};";
            }
            output.set(result);
            context.write(output, empty);
        }
    }
}