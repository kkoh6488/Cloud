package Cloud;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserCountReducer extends Reducer<PlaceJoinKey, Text, Text, NullWritable> {

    Text output = new Text();
    private String placeData;
    private String outputRow;
    private String thisUser, lastUser = "";

    @Override
    public void reduce(PlaceJoinKey joinKey, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for (Text t : values) {
            output.set(joinKey.toString() + "\t" + t.toString());
            context.write(output, NullWritable.get());
        }
        /*
        // If it's from the places.txt file
        if (joinKey.value.equals("0")) {
            placeData = values.iterator().next().toString();
            int firstTab = placeData.indexOf("\t");
            placeData = placeData.substring(firstTab + 2);          // Remove the placeID from being output
        } else {
            for (Text t : values) {
                thisUser = t.toString();
                if (lastUser.equals(thisUser)) {
                    continue;
                }
                lastUser = thisUser;
                outputRow = placeData + "\t" + thisUser;
                output.set(outputRow);
                context.write(output, NullWritable.get());
            }
        }
        */
    }
}