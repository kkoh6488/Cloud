package Cloud;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/* Gets the top 10 localities per country by emitting sorted rows */
public class TopLocaleMapper extends Mapper<Object, Text, TopLocaleKey, NullWritable> {
    private IntWritable count = new IntWritable();
    NullWritable empty = NullWritable.get();

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] dataArray = value.toString().split("\t"); //split the data into array
        if (dataArray.length < 4) {
            //  record with incomplete data
            return; // don't emit anything
        }
        String country = dataArray[0];
        String locale = dataArray[1];
        String neighbourhood = dataArray[2];
        int uniqueCount = Integer.parseInt(dataArray[3]);
        count.set(uniqueCount);
        if (neighbourhood.equals("#")) {
            country = "1" + country;
        } else {
            country = "0" + country;
        }
        context.write(new TopLocaleKey(country, locale, neighbourhood, uniqueCount), empty);
    }
}
