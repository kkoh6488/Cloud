package Cloud;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// Counts the number of users per place and joins the place table.
public class UserCountReducer extends Reducer<PlaceJoinKey, Text, Text, NullWritable> {

    Text output = new Text();
    private String placeData;
    private String currentPlaceId;

    @Override
    public void reduce(PlaceJoinKey joinKey, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text t : values) {
            if (joinKey.value.toString().equals("0")) {                    // If it's from the place file
                currentPlaceId = joinKey.placeID.toString();
                placeData = t.toString();
                int firstTab = placeData.indexOf("\t");                    // Remove the place ID from the output string
                placeData = placeData.substring(firstTab + 1);
            } else {
                if (joinKey.placeID.toString().equals(currentPlaceId)) {   // If this place has an ID with it, join and write it.
                    output.set(placeData + "\t" + t.toString());
                    context.write(output, NullWritable.get());
                } else {
                    currentPlaceId = "";
                }
            }
        }
    }
}