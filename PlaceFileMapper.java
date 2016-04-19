package Cloud;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

// Outputs neighbourhoods and locales from the places.txt file.
public class PlaceFileMapper extends Mapper<Object, Text, PlaceJoinKey, Text> {
    private Text output = new Text();
    private String result;
    PlaceJoinKey joinKey;

    private NullWritable empty = NullWritable.get();

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] dataArray = value.toString().split("\t"); //split the data into array
        if (dataArray.length < 7)
        {
            //  record with invalid data
            return; // don't emit anything
        }
        String placeId = dataArray[0];
        String placeName = dataArray[4];
        String type = dataArray[5];

        String country, locality, neighbourhood = "#";

        int firstCommaIndex = placeName.indexOf(",");
        int lastCommaIndex = placeName.lastIndexOf(",");
        country = placeName.substring(lastCommaIndex + 2);

        if (type.equals("7")) {
            locality = placeName.substring(0, firstCommaIndex);
        } else if (type.equals("22")) {
            int secondcomma = placeName.indexOf(',', firstCommaIndex + 2);
            locality = placeName.substring(firstCommaIndex + 2, secondcomma);
            neighbourhood = placeName.substring(0, firstCommaIndex);
        }
        else {
            return;
        }
        result = placeId + "\t" + country + "\t" + locality + "\t" + neighbourhood;
        joinKey = new PlaceJoinKey(placeId, "0");
        output.set(result);
        context.write(joinKey, output);
    }
}
