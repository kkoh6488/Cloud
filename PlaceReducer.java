package Cloud;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PlaceReducer extends Reducer<LocalityKey, Text, Text, IntWritable> {

    private Text print = new Text();
    private String output;
    private PlacePair tempPair;
    private HashMap<PlacePair, HashSet<Text>> localityUsers = new HashMap<PlacePair, HashSet<Text>();
    private HashSet<Text> seenUsers;
    private IntWritable count = new IntWritable();

    @Override
    public void reduce(LocalityKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        tempPair = new PlacePair(key.getLocale(), key.getCountry());
        int counter = 0;

        if (localityUsers.containsKey(tempPair)) {
            seenUsers = localityUsers.get(tempPair);
        } else {
            seenUsers = new HashSet<Text>();
            localityUsers.put(tempPair, seenUsers);
        }

        for (Text t : values) {
            if (!seenUsers.contains(t)) {
                seenUsers.add(t);
            }
            counter++;
        }

        // If it's a neighbourhood - sum it and output
        if (!key.getNeighbourhood().equals("#")) {
            print.set(key.toString());
            count.set(counter);
            context.write(print, count);
        }
    }

    /// For each locality (only), print out the number of distinct users including neighbourhood unique users (no duplicates)
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (PlacePair p : localityUsers.keySet())
        {
            output = p.country + "\t" + p.locale + "\t#\t";
            print.set(output);
            count.set(localityUsers.get(p).size());
            context.write(print, count);
        }
    }

}