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
public class LocalityKey implements WritableComparable {
	private Text localityName = new Text();
	private Text placeID = new Text();
	private Text countryName = new Text();
	private Text neighborhoodName = new Text();
	private IntWritable uniqueUsers = new IntWritable();

	// Default constructor
	public LocalityKey() {}

	public LocalityKey(String placeId, String country, String locality, String neighborhood, int uniqueUsers)
	{
		this.localityName.set(locality);
		this.placeID.set(placeId);
		this.countryName.set(country);
		this.neighborhoodName.set(neighborhood);
		this.uniqueUsers.set(uniqueUsers);
	}

    @Override
	public int compareTo(Object o)
	{
        LocalityKey lk = (LocalityKey) o;
		/*
        if (countryName.equals(lk.countryName) && localityName.equals(lk.localityName))
		{
            if (neighborhoodName != null && lk.neighborhoodName != null &&
					neighborhoodName.equals(lk.neighborhoodName))
			{
                return 0;
            }
        }
        */
		// Sort based on country name, then by number of unique users
		int compare = countryName.compareTo(lk.countryName);
		if (compare == 0) {
			return lk.uniqueUsers.compareTo(uniqueUsers);
		}
		return compare;
        //return -1;
		//return placeID.toString().compareTo(lk.toString());
	}

	@Override
	public String toString()
	{
		/*
        if (!localityName.toString().equals(""))
        {
			if (!neighborhoodName.toString().equals((""))) {
				return countryName.toString() + "\t" + localityName.toString() + "\t" + neighborhoodName.toString();
			}
        }
        */
		return countryName.toString() + "\t" + localityName.toString() + "\t" + neighborhoodName.toString();
		//return countryName.toString() + "\t" + neighborhoodName.toString();
    }

	/* To entries are equal if they have the same locality AND neighbourhood AND country

	 */
	@Override
	public boolean equals(Object o)
	{
		if(o instanceof LocalityKey)
		{
			LocalityKey lk = (LocalityKey) o;
			return localityName.equals(lk.localityName) && countryName.equals(lk.countryName)
						&& neighborhoodName.equals(lk.neighborhoodName);
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

    public String getCountry()
    {
        return countryName.toString();
    }

	public IntWritable getUniqueUsers() { return uniqueUsers; }
	
	@Override
	//overriding default readFields method. 
	//It de-serializes the byte stream data
	public void readFields(DataInput in ) throws IOException {
	    localityName.readFields( in );
	    placeID.readFields( in );
		neighborhoodName.readFields( in );
		countryName.readFields( in );
		uniqueUsers.readFields( in );
	}

	@Override
  	//It serializes object data into byte stream data
  	public void write(DataOutput out) throws IOException {
	    localityName.write( out );
		placeID.write( out );
		neighborhoodName.write( out );
		countryName.write( out );
		uniqueUsers.write( out );
  	}
}
