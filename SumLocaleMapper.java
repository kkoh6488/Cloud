package Cloud;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

// Mapper <inputkey, inputvalue, outputkey, outputvalue>
public class SumLocaleMapper extends Mapper<Object, Text, SummedPlaceKey, IntWritable> {
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
        if (neighbourhood.equals("#")) {
            country = "1" + country;
        } else {
            country = "0" + country;
        }
        context.write(new SummedPlaceKey(country, locale, neighbourhood, uniqueCount), count);
    }
}
