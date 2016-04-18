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
        String locale = placeKey.getLocale();
        result = placeKey.getCountry() + "\t{(" + placeKey.getLocale() + ":" + placeKey.getUniqueUsers() + placeKey.getNeighbourhood() + ")};";
        output.set(result);
        context.write(output, empty);
        /*
        if (placeKey.getLocale().charAt(0) == '0') {
            lastNB = ", " + placeKey.getNeighbourhood() + ":" + placeKey.getUniqueUsers();
            lastLocale = locale;
            return;
        }
        else {
            if (locale.equals(lastLocale))
            {
                result = placeKey.getCountry() + "\t{(" + placeKey.getLocale() + ":" + placeKey.getUniqueUsers() + lastNB + ")};";
                lastNB = "";
                lastLocale = "";
            } else {
                result = placeKey.getCountry() + "\t{(" + placeKey.getLocale() + ":" + placeKey.getUniqueUsers() + ")};";
            }
            output.set(result);
            context.write(output, empty);
        }
        */
    }
}