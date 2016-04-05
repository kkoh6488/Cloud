package Cloud;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

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
	private Text ownerID = new Text();

	// Default constructor
	public LocalityKey() {}

	public LocalityKey(String placeId, String country, String locality, String neighborhood, String owner)
	{
		localityName.set(locality);
		placeID.set(placeId);
		countryName.set(country);
		neighborhoodName.set(neighborhood);
		ownerID.set(owner);
	}
	
	public int compareTo(Object o)
	{
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
		LocalityKey lk = (LocalityKey) o;
		return placeID.toString().compareTo(lk.toString());
	}

	@Override
	public String toString()
	{
		return placeID.toString();
	}

	@Override
	public boolean equals(Object o)
	{
		if(o instanceof LocalityKey)
		{
			LocalityKey lk = (LocalityKey) o;
			return placeID.equals(lk.placeID);
		}
		return false;
	}
	public String getLocale()
	{
		return localityName.toString();
	}
	
	@Override
	//overriding default readFields method. 
	//It de-serializes the byte stream data
	public void readFields(DataInput in ) throws IOException {
	    localityName.readFields( in );
	    placeID.readFields( in );
		neighborhoodName.readFields( in );
		countryName.readFields( in );
		ownerID.readFields( in );
	}

	@Override
  	//It serializes object data into byte stream data
  	public void write(DataOutput out) throws IOException {
	    localityName.write( out );
		placeID.write( out );
		neighborhoodName.write( out );
		countryName.write( out );
		ownerID.write( out );
  	}
}
