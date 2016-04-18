package Cloud;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

// Creates a key based off the placeID and userID to eliminate duplicate pairs when sent to reducer.
public class UserCountMapper extends Mapper<Object, Text, Text, NullWritable> {
    private Map<UserPlaceKey, String> userCounts = new HashMap<UserPlaceKey, String>();
    private Text mapKey = new Text();
    private String lastSeenKey = "", thisKey = "";

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] dataArray = value.toString().split("\t"); //split the data into array
        if (dataArray.length < 6) {
            //  record with incomplete data
            return; // don't emit anything
        }
        String userId = dataArray[1];
        String placeId = dataArray[4];
        thisKey = placeId + "\t" + userId;
        if (thisKey.equals(lastSeenKey)) {
            return;
        }
        mapKey.set(thisKey);
        lastSeenKey = thisKey;
        context.write(mapKey, NullWritable.get());
    }
}
