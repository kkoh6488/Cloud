package Cloud;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SumLocaleReducer extends Reducer<SummedPlaceKey, IntWritable, Text, NullWritable> {
    HashMap<PlacePair, String> topNeighbourhoods = new HashMap<PlacePair, String>();
    HashMap<String, Integer> topCounts = new HashMap<String, Integer>();    // Gets the top N per country
    PlacePair tempPair;
    Text output = new Text();
    String result;
    NullWritable empty = NullWritable.get();
    private final int TOP_N = 8;    // Make this 10 later
    String lastNB = "";

    @Override
    public void reduce(SummedPlaceKey placeKey, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // Emit the sorted rows - but only the top neighbourhood
        if (placeKey.getCountry().charAt(0) == '0' && lastNB.equals(placeKey.getNeighbourhood())) {
            return;
        }
        result = placeKey.getCountry() + "\t" + placeKey.getLocale() + "\t" + placeKey.getNeighbourhood() + "\t" + placeKey.getUniqueUsers();
        output.set(result);
        lastNB = placeKey.getNeighbourhood();
        context.write(output, empty);
    }

    /*
    @Override
    public void reduce(SummedPlaceKey placeKey, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        String country = placeKey.getCountry().substring(1);
        String locale = placeKey.getLocale();
        tempPair = new PlacePair(locale, country);

        // If it's a neighbourhood - assume the first one seen for a place pair has the most uniques
        if (placeKey.getCountry().charAt(0) == '0') {

            result = "NB Contains Key:" + topNeighbourhoods.containsKey(tempPair) + ", " + country + "\t{(" + locale + ":"
                    + placeKey.getUniqueUsers() + ", " + placeKey.getNeighbourhood() + ":" + placeKey.getUniqueUsers() + ")};";

            if (!topNeighbourhoods.containsKey(tempPair)) {
                topNeighbourhoods.put(tempPair, placeKey.getNeighbourhood() + ":" + placeKey.getUniqueUsers());
            }
        }
        else {
            // If it's a locale - get the top 10 for each country. Otherwise, skip this locale.
            // Assume that the first one is the locale with the most unique users for a given country
            if (topCounts.containsKey(country)) {
                int numLocs = topCounts.get(country);
                if (numLocs < TOP_N) {
                    topCounts.put(country, numLocs + 1);
                } else {
                    return;
                }
            } else {
                topCounts.put(country, 0);
            }

            String nb = "";
            if (topNeighbourhoods.containsKey(tempPair)) {
                nb = topNeighbourhoods.get(tempPair);
            }
            result = "LOC, has NH :" + topNeighbourhoods.containsKey(tempPair) + ", " + country + "\t{(" + locale + ":" + placeKey.getUniqueUsers() + ", " + nb + ")};";
            output.set(result);
            context.write(output, empty);
        }
    }
    */
}