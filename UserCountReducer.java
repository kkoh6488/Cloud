package Cloud;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserCountReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

    @Override
    public void reduce(Text placeKey, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        /*
        int count = 0;
        for (Text val: values) {
            count += 1;
        }
        */
        //context.write(placeKey.placeID, placeKey.user);
        context.write(placeKey, NullWritable.get());
        //for (Text user : values) {
        //    context.write(placeID, user);
        //}
    }
}