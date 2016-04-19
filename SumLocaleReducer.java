package Cloud;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SumLocaleReducer extends Reducer<SummedPlaceKey, NullWritable, Text, NullWritable> {
    Text output = new Text();
    String result;
    NullWritable empty = NullWritable.get();
    //String lastLocale = "";
    String lastCountry = "";
    int TOP_N = 10;
    int counter = 1;

    @Override
    public void reduce(SummedPlaceKey placeKey, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        // Emit the sorted rows - but only the top 10 locales per country
        //if (placeKey.getCountry().charAt(0) == '0' && lastLocale.equals(placeKey.getLocale())) {
        //    return;
        //}
        if (placeKey.getCountry().charAt(0) == '1' && lastCountry.equals(placeKey.getCountry())) {
            if (counter >= TOP_N) {
                return;
            } else {
                counter++;
            }
        } else {
            counter = 1;
        }
        result = placeKey.getCountry() + "\t" + placeKey.getLocale() + "\t" + placeKey.getNeighbourhood() + "\t" + placeKey.getUniqueUsers();
        output.set(result);
        //lastLocale = placeKey.getLocale();
        lastCountry = placeKey.getCountry();
        context.write(output, empty);
    }
}