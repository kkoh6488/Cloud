package Cloud;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

// Mapper <inputkey, inputvalue, outputkey, outputvalue>
public class TopLocaleMapper extends Mapper<Object, Text, TopLocaleKey, IntWritable> {
    private IntWritable count = new IntWritable();
    private String lastCountry = "";
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

        String countryWithoutFlag = country.substring(1);

        // If it's a locale, only write the top 10 for each country
        if (country.charAt(0) == '1') {
            if (lastCountry.equals(countryWithoutFlag)) {
                if (counter < TOP_N) {
                    counter++;
                } else {
                    return;
                }
            } else {
                // Reset the counters
                counter = 0;
            }
            country = countryWithoutFlag;     // Remove the flag for locale
            lastCountry = country;                // Store the locale of the previous row
            locale = locale + "1";              // Add flag so locales will be after the top NB
        }
        else {
            country = countryWithoutFlag;             // Remove the flag for NB
            locale = locale + "0";                      // Add flag so NB will appear first
        }
        context.write(new TopLocaleKey(country, locale, neighbourhood, uniqueCount), count);
    }
}
