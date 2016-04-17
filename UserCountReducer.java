package Cloud;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserCountReducer extends Reducer<Text, Text, Text, Text> {
    //private Map<String, Integer> placeCounts = new HashMap<String, Integer>();

    @Override
    public void reduce(Text placeID, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        /*
        int count = 0;
        for (Text val: values) {
            count += 1;
        }
        */
        context.write(placeID, values.iterator().next());
    }
}