package Cloud;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopLocaleReducer extends Reducer<TopLocaleKey, IntWritable, Text, NullWritable> {
    Text output = new Text();
    String result;
    NullWritable empty = NullWritable.get();
    String lastNB = "", lastLocale = "";

    @Override
    public void reduce(TopLocaleKey placeKey, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // For each locale, get the top neighbourhood for it - should be the previous row
        if (placeKey.getLocale().charAt(0) == '0') {
            lastNB = ", " + placeKey.getNeighbourhood() + ":" + placeKey.getUniqueUsers();
            return;
        }
        else {
            result = placeKey.getCountry() + "\t{(" + placeKey.getLocale() + ":" + placeKey.getUniqueUsers() + lastNB + ")};";
            output.set(result);
            context.write(output, empty);
            lastNB = "";
        }
    }
}