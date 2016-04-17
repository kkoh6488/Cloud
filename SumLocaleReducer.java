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
        String country = placeKey.getCountry().substring(1);
        String locale = placeKey.getLocale();
        tempPair = new PlacePair(locale, country);

        // If it's a neighbourhood - assume the first one seen for a place pair has the most uniques
        if (placeKey.getCountry().charAt(0) == '0') {

            result = "NB Contains Key:" + topNeighbourhoods.containsKey(tempPair) + ", " + country + "\t{(" + locale + ":"
                    + placeKey.getUniqueUsers() + ", " + placeKey.getNeighbourhood() + ":" + placeKey.getUniqueUsers() + ")};";

            if (!topNeighbourhoods.containsKey(tempPair)) {
                topNeighbourhoods.put(tempPair, placeKey);
            }

            //    topNeighbourhoods.put(tempPair, placeKey);
            //}
        }
        else {
            result = "LOC, has NH :" + topNeighbourhoods.containsKey(tempPair) + ", " + country + "\t{(" + locale + ":" + placeKey.getUniqueUsers() + ")};";
        }
        output.set(result);            //if (!topNeighbourhoods.containsKey(tempPair)) {
        context.write(output, empty);

        /*
        //} else {
            // If it's a locale - check its flag
        else if (placeKey.getCountry().charAt(0) == '1') {

            if (topNeighbourhoods.containsKey(tempPair)) {
                SummedPlaceKey topNB = topNeighbourhoods.get(tempPair);
                result = country + "\t{(" + locale + ":"
                        + placeKey.getUniqueUsers() + ", " + topNB.getNeighbourhood() + ":" + topNB.getUniqueUsers() + ")};";
                output.set(result);
            } else {
                result = country + "\t{(" + locale + ":" + placeKey.getUniqueUsers() + ")};";
                output.set(result);
            }
            context.write(output, empty);
        }

        /*

        // If it's a neighbourhood - store the top result per locality
        if (!placeKey.getNeighbourhood().equals("#")) {
            if (!topNeighbourhoods.containsKey(tempPair)) {         // Only get the first one - since they are sorted
                topNeighbourhoods.put(tempPair, placeKey);
            }
        } else {
            // If it's a locale - get the top 10 for each country. Otherwise, skip this locale.
            // Assume that the first one is the locale with the most unique users for a given country
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
        */
    }
}