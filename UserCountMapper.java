package Cloud;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.HashSet;

/* Creates a key based off the placeID and userID to eliminate
duplicate pairs before being sent to the reducer. */
public class UserCountMapper extends Mapper<Object, Text, PlaceJoinKey, Text> {
    private Text output = new Text();
    private String lastSeenKey = "", thisKey = "";
    PlaceJoinKey joinKey;
    HashSet<String> seenKeys = new HashSet<String>();

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] dataArray = value.toString().split("\t");
        if (dataArray.length < 6) { //  record with incomplete data
            return;
        }
        String userId = dataArray[1];
        String placeId = dataArray[4];
        thisKey = placeId + "\t" + userId;

        // Don't emit multiple photos for a place by the same user.
        if (thisKey.equals(lastSeenKey) || seenKeys.contains(thisKey)) {
            return;
        }
        output.set(userId);
        lastSeenKey = thisKey;
        seenKeys.add(lastSeenKey);
        joinKey = new PlaceJoinKey(placeId, "1");
        context.write(joinKey, output);
    }
}
