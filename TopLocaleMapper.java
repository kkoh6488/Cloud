package Cloud;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

// Mapper <inputkey, inputvalue, outputkey, outputvalue>
public class TopLocaleMapper extends Mapper<Object, Text, TopLocaleKey, IntWritable> {
    private IntWritable count = new IntWritable();
    private String lastLocale = "";
    private int counter = 0;
    private final int TOP_N = 7;

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

        // If it's a locale, only write the top 10
        if (country.charAt(0) == '1') {
            if (lastLocale.equals(locale)) {
                if (counter < TOP_N) {
                    counter++;
                } else {
                    return;
                }
            } else {
                // Reset the counters
                counter = 0;
            }
            country = country.substring(1);     // Remove the flag for locale
            lastLocale = locale;                // Store the locale of the previous row
            locale = "1" + locale;              // Add flag so locales will be after the top NB
        }
        else {
            country = country.substring(1);             // Remove the flag for NB
            locale = "0" + locale;                      // Add flag so NB will appear first
        }
        context.write(new TopLocaleKey(country, locale, neighbourhood, uniqueCount), count);
    }
}
