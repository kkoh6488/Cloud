package Cloud;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SumLocaleReducer extends Reducer<SummedPlaceKey, IntWritable, Text, Text> {
    HashMap<PlacePair, SummedPlaceKey> topNeighbourhoods = new HashMap<PlacePair, SummedPlaceKey>();
    PlacePair tempPair;
    Text empty = new Text();
    Text output = new Text();
    String result;

    @Override
    public void reduce(SummedPlaceKey placeKey, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        tempPair = new PlacePair(placeKey.getCountry(), placeKey.getLocale());
        if (!placeKey.getNeighbourhood().equals("#")) {
            if (!topNeighbourhoods.containsKey(tempPair)) {         // Only get the first one - since they are sorted
                topNeighbourhoods.put(tempPair, placeKey);
            }
        } else {
            SummedPlaceKey topNB = topNeighbourhoods.get(tempPair);
            result = placeKey.getCountry() + "\t{(" + placeKey.getLocale() + ":"
                    + placeKey.getUniqueUsers() + ", " + topNB.getNeighbourhood() + ":" + topNB.getUniqueUsers();
            output.set(result);
            context.write(output, empty);
        }
    }
}