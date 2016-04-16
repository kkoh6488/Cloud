package Cloud;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

// Mapper <inputkey, inputvalue, outputkey, outputvalue>
public class TopNMapper extends Mapper<Object, Text, Text, IntWritable> {
    private Map<UserPlaceKey, Integer> userCounts = new HashMap<UserPlaceKey, Integer>();
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
            userCounts.put(userKey, 1);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (UserPlaceKey u : userCounts.keySet()) {
            context.write(u.placeID, ONE);
        }
    }
}
