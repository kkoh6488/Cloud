package Cloud;

import java.io.IOException;

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

        String locale = placeKey.getLocale();
        if (locale.charAt(locale.length() - 1) == '0') {
            lastLocale = locale.substring(0, locale.length() - 1);     // Remove the flag
            lastNB = ", " + placeKey.getNeighbourhood() + ":" + placeKey.getUniqueUsers();
        } else {
            // For each locale, get the top neighbourhood for it - should be the previous row
            locale = locale.substring(0, locale.length() - 1);
            if (locale.equals(lastLocale)) {
                result = placeKey.getCountry() + "\t{(" + locale + ":" + placeKey.getUniqueUsers() + lastNB + ")};";
                lastLocale = "";
            } else {
                result = placeKey.getCountry() + "\t{(" + locale + ":" + placeKey.getUniqueUsers() + ")};";
            }
            output.set(result);
            context.write(output, empty);
        }
    }
}