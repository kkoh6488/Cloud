package Cloud;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/* Joins each top neighbourhood with its associated locale. */
public class TopNBReducer extends Reducer<TopNBKey, NullWritable, Text, NullWritable> {
    Text output = new Text();
    String result;
    NullWritable empty = NullWritable.get();
    String lastNB = "", lastLocale = "";

    @Override
    public void reduce(TopNBKey placeKey, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        String locale = placeKey.getLocale();
        char flag = locale.charAt(locale.length() - 1);
        if (flag == '0') {
            locale = locale.substring(0, locale.length() - 1);
            if (!locale.equals(lastLocale)) {
                lastNB = ", " + placeKey.getNeighbourhood() + ":" + placeKey.getUniqueUsers();
                lastLocale = locale;
            } else {
                return;
            }
        } else {
            // If there's a top neighbourhood for this locale
            locale = locale.substring(0, locale.length() - 1);
            if (locale.equals(lastLocale)) {
                result = placeKey.getCountry() + "\t{(" + locale + ":" + placeKey.getUniqueUsers() + lastNB + ")};";
            } else {
                // Otherwise there isn't a top NB
                result = placeKey.getCountry() + "\t{(" + locale + ":" + placeKey.getUniqueUsers() + ")};";
            }
            output.set(result);
            context.write(output, empty);
        }
    }
}