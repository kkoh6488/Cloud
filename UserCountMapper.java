package Cloud;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

// Mapper <inputkey, inputvalue, outputkey, outputvalue>
public class UserCountMapper extends Mapper<Object, Text, Text, Text> {
    private Map<UserPlaceKey, String> userCounts = new HashMap<UserPlaceKey, String>();
    private static final IntWritable ONE = new IntWritable(1);
    private UserPlaceKey userKey;

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
        userKey = new UserPlaceKey(userId, placeId);

        // Get rid of duplicate users for each place
        if (!userCounts.containsKey(key)) {
            userCounts.put(userKey, userId);
        }
    }

    // Output distinct place id, user id pairs
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (UserPlaceKey u : userCounts.keySet()) {
            context.write(u.placeID, u.user);
        }
    }
}
