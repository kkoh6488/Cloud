package Cloud;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserCountReducer extends Reducer<UserPlaceKey, NullWritable, Text, Text> {

    @Override
    public void reduce(UserPlaceKey placeKey, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        /*
        int count = 0;
        for (Text val: values) {
            count += 1;
        }
        */
        context.write(placeKey.placeID, placeKey.user);
        //for (Text user : values) {
        //    context.write(placeID, user);
        //}
    }
}