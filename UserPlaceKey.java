package Cloud;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Ken on 16/04/2016.
 */
public class UserPlaceKey implements WritableComparable{

    Text user;
    Text placeID;

    public UserPlaceKey(){}

    public UserPlaceKey(String user, String placeID) {
        this.user = new Text(user);
        this.placeID = new Text(placeID);
    }

    @Override
    public int compareTo(Object o) {
        UserPlaceKey k = (UserPlaceKey) o;
        if (user.equals(k.user) && placeID.equals(k.placeID)) {
            return 0;
        } else {
            int result = placeID.compareTo(k.placeID);
            if (result == 0) {
                result = user.compareTo(k.user);
            }
            return result;
        }
    }

    @Override
    public String toString()
    {
        return user.toString() + "," + placeID.toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if(o instanceof UserPlaceKey)
        {
            UserPlaceKey k = (UserPlaceKey) o;
            return user.equals(k.user) && placeID.equals(k.placeID);
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return user.hashCode() + placeID.hashCode();
    }

    @Override
    //overriding default readFields method.
    //It de-serializes the byte stream data
    public void readFields(DataInput in ) throws IOException {
        user.readFields( in );
        placeID.readFields( in );
    }

    @Override
    //It serializes object data into byte stream data
    public void write(DataOutput out) throws IOException {
        user.write( out );
        placeID.write( out );
    }
}
