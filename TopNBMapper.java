package Cloud;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/* Emits keys so that top neighbourhoods appear above their locality. */
public class TopNBMapper extends Mapper<Object, Text, TopNBKey, NullWritable> {
    private IntWritable count = new IntWritable();

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

        if (country.charAt(0) == '1') {
            locale = locale + "1";                      // Add flag so locales will be after the top NB
        }
        else {
            locale = locale + "0";                      // Add flag so NB will appear first
        }
        country = country.substring(1);                 // Remove the flag for the country
        context.write(new TopNBKey(country, locale, neighbourhood, uniqueCount), NullWritable.get());
    }
}
