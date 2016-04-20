package Cloud;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/*  Key used for joining place file with photo records, by placeId. */
public class PlaceJoinKey implements WritableComparable{

    Text value;
    Text placeID;

    public PlaceJoinKey(){
        value = new Text();
        placeID = new Text();
    }

    public PlaceJoinKey(String placeID, String value) {
        this.value = new Text(value);
        this.placeID = new Text(placeID);
    }

    /* Sorts by the placeID and then by flag value */
    @Override
    public int compareTo(Object o) {
        PlaceJoinKey k = (PlaceJoinKey) o;
        if (value.equals(k.value) && placeID.equals(k.placeID)) {
            return 0;
        } else {
            int result = placeID.compareTo(k.placeID);
            if (result == 0) {
                result = value.compareTo(k.value);
            }
            return result;
        }
    }

    @Override
    public String toString()
    {
        return placeID.toString() + "\t" + value.toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if(o instanceof PlaceJoinKey)
        {
            PlaceJoinKey k = (PlaceJoinKey) o;
            return value.equals(k.value) && placeID.equals(k.placeID);
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return placeID.hashCode();
    }

    @Override
    public void readFields(DataInput in ) throws IOException {
        value.readFields( in );
        placeID.readFields( in );
    }

    @Override
    //It serializes object data into byte stream data
    public void write(DataOutput out) throws IOException {
        value.write( out );
        placeID.write( out );
    }
}
