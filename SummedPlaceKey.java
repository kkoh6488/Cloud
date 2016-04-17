package Cloud;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/* Key for locality data.
 * Will be passed to the combiner for aggregation.
 */
public class SummedPlaceKey implements WritableComparable {
    private Text localityName = new Text();
    private Text countryName = new Text();
    private Text neighborhoodName = new Text();
    private IntWritable uniqueUsers = new IntWritable();

    // Default constructor
    public SummedPlaceKey() {}

    public SummedPlaceKey(String country, String locality, String neighborhood, int uniqueUsers)
    {
        this.localityName.set(locality);
        this.neighborhoodName.set(neighborhood);
        this.uniqueUsers.set(uniqueUsers);
        this.countryName.set(country);

        // Append a 0 or 1 to indicate if this is a locality (1) or neighbourhood (0).
        // 0 for neighbourhoods because they need to be read first.
        // Need to do this so that neighbourhoods are seen first in reduce. (Is this how reducer gets inputs??)
        // Then we can pick the top 10 and get the neighbourhoods at the same time.
        if (neighborhoodName.equals("#")) {
            this.countryName.set("1" + country);
        } else {
            this.countryName.set("0" + country);
        }
    }

    @Override
    public int compareTo(Object o)
    {
        SummedPlaceKey k = (SummedPlaceKey) o;
        // Sort based on country name, then by number of unique users (descending).
        int compare = countryName.compareTo(k.countryName);
        if (compare == 0) {
            compare = k.uniqueUsers.compareTo(uniqueUsers);
            if (compare == 0) {
                compare = localityName.compareTo(k.localityName);
            }
        }
        return compare;
    }

    @Override
    public String toString()
    {
        return countryName.toString() + "\t" + localityName.toString() + "\t" + neighborhoodName.toString();
    }

    /* To entries are equal if they have the same locality AND neighbourhood AND country

     */
    @Override
    public boolean equals(Object o)
    {
        if(o instanceof SummedPlaceKey)
        {
            SummedPlaceKey k = (SummedPlaceKey) o;
            return localityName.equals(k.localityName) && countryName.equals(k.countryName)
                    && neighborhoodName.equals(k.neighborhoodName);
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return localityName.hashCode() + countryName.hashCode() + neighborhoodName.hashCode();
    }

    public String getLocale()
    {
        return localityName.toString();
    }

    // Strips out the index used for sorting of locality / neighbourhoods
    public String getCountry()
    {
        return countryName.toString().substring(1);
    }

    public IntWritable getUniqueUsers() { return uniqueUsers; }

    public String getNeighbourhood() { return neighborhoodName.toString(); }

    @Override
    //overriding default readFields method.
    //It de-serializes the byte stream data
    public void readFields(DataInput in ) throws IOException {
        localityName.readFields( in );
        neighborhoodName.readFields( in );
        countryName.readFields( in );
        uniqueUsers.readFields( in );
    }

    @Override
    //It serializes object data into byte stream data
    public void write(DataOutput out) throws IOException {
        localityName.write( out );
        neighborhoodName.write( out );
        countryName.write( out );
        uniqueUsers.write( out );
    }
}
