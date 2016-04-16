package Cloud;

import java.util.Objects;

/**
 * Created by Ken on 13/04/2016.
 */
public class PlacePair {

    String locale;
    String country;

    public PlacePair(String loc, String country) {
        locale = loc;
        country = country;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        else {
            if (!(o instanceof PlacePair)) {
                return false;
            }
            PlacePair p = (PlacePair) o;
            return p.locale.equals(this.locale) && p.country.equals(this.country);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(locale, country);
    }

}
