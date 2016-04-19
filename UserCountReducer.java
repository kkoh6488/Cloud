package Cloud;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserCountReducer extends Reducer<PlaceJoinKey, Text, Text, NullWritable> {

    Text output = new Text();
    private String placeData;
    private String outputRow;
    private String thisUser, lastUser = "", currentPlaceId;

    @Override
    public void reduce(PlaceJoinKey joinKey, Iterable<Text> values, Context context) throws IOException, InterruptedException {


        for (Text t : values) {
            output.set(joinKey.toString() + "\t" + t.toString());
            context.write(output, NullWritable.get());

            if (joinKey.value.equals("0")) {                    // If it's from the place file
                currentPlaceId = joinKey.placeID.toString();
                int firstTab = placeData.indexOf("\t");
                placeData = t.toString().substring(firstTab + 2);
            } else {
                if (joinKey.placeID.equals(currentPlaceId)) {   // If this place has an ID with it
                    output.set(placeData + "\t" + t.toString());
                    context.write(output, NullWritable.get());
                } else {
                    currentPlaceId = "";
                }
            }

        }
        //if (!localeForNB.equals(joinKey.))

        /*
        // If it's from the places.txt file
        if (joinKey.value.equals("0")) {
            for (Text t : values) {
                placeData = t.toString();
            }
            int firstTab = placeData.indexOf("\t");
            currentPlaceId = placeData.substring(0, firstTab);
            placeData = placeData.substring(firstTab + 2) + "\t";          // Remove the placeID from being output
        } else {
            for (Text t : values) {
                thisUser = t.toString();
                lastUser = thisUser;
                outputRow = placeData + thisUser;
                if (currentPlaceId.equals(joinKey.placeID)) {
                    outputRow = "HasPlace-" + outputRow;
                }
                output.set(outputRow);
                context.write(output, NullWritable.get());
            }
        }
        */
    }
}