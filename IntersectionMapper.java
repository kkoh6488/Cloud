package Cloud;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

// Mapper <inputkey, inputvalue, outputkey, outputvalue>
public class IntersectionMapper extends Mapper<Object, Text, LocalityKey, Text> {
    private LocalityKey localeKey;
    private Text output = new Text();

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] dataArray = value.toString().split("\t"); //split the data into array
        if (dataArray.length < 4 )
        {
            //  record with invalid data
            return; // don't emit anything
        }
        String country = dataArray[0];
        String locality = dataArray[1];
        String neighborhood = dataArray[2];
        String userId = dataArray[3];

        localeKey = new LocalityKey(country, locality, neighborhood);
        output.set(userId);
        context.write(localeKey, output);

        //Context write - to do set intersection - emit another record for with key just for locale,
        // Then in the reduce, if the key is a locale, do a normal count but don't include duplicates
        if (!neighborhood.equals("#")) {
            localeKey = new LocalityKey(country, locality, "#");
            context.write(localeKey, output);
        }
    }
}
