package Cloud.Unused;

import java.io.IOException;
import java.util.HashSet;

import Cloud.LocalityKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PlaceReducer extends Reducer<LocalityKey, Text, Text, IntWritable> {
    private Text print = new Text();
    // Checks for duplicate users for a place. This is needed due to the double emitting from the last map.
    private HashSet<Text> seenUsers = new HashSet<>();
    private IntWritable count = new IntWritable();

    @Override
    public void reduce(LocalityKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int counter = 0;
        seenUsers.clear();
        for (Text t : values) {
            if (!seenUsers.contains(t)) {
                seenUsers.add(t);
                counter++;
            }
        }
        print.set(key.toString());
        count.set(counter);
        context.write(print, count);
    }
}