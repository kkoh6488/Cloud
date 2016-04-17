package Cloud;

import junit.framework.TestCase;

/**
 * Created by Ken on 5/04/2016.
 */
public class LocalityKeyTest extends TestCase {
    private LocalityKey l1, l2, l3, l4emptyLoc, f2, f3, f4, f5;

    // Constructor reference
    // public LocalityKey(String placeId, String country, String locality, String neighborhood, String owner)

    public void setUp() throws Exception {
        super.setUp();
        l1 = new LocalityKey("xbxI9VGYA5oZH8tLJA", "United Kingdom", "Coventry City", "#", 1);
        l2 = new LocalityKey("xbxI9VGYA5oZH8tLJA", "United Kingdom", "Coventry City", "#", 2);
        l3 = new LocalityKey("qd2lIgybBZVRy1zw", "United States", "Seligman", "#", 1);
        //l4emptyLoc = new LocalityKey("qd2lIgybBZVRy1zw", "United States", "\t \t", "\t \t", 4);

        f2 = new LocalityKey("placeID-Loc4-2", "Country4", "Locale4-2", "#", 3);
        f3 = new LocalityKey("placeID-Loc4-3", "Country4", "Locale4-3", "#", 2);
        f4 = new LocalityKey("placeID-Loc4-4", "Country4", "Locale4-4", "#", 2);
        f5 = new LocalityKey("placeID-Loc4-5", "Country4", "Locale4-5", "#", 3);

    }

    public void testEqualsFake() throws Exception {
        assertEquals(false, f2.equals(f3));
    }

    public void testEqualsFakeSameUniques() throws Exception {
        assertEquals(false, f3.equals(f4));
    }

    public void testCompareTo() throws Exception {
        assertEquals(new Integer(2).compareTo(new Integer(1)), l1.compareTo(l2));
    }

    public void testToString() throws Exception {
        assertEquals("United Kingdom\tCoventry City\t#", l1.toString());
    }

    /*
    public void testNoLocaleToString() throws Exception {
        //assertEquals("qd2lIgybBZVRy1zw", l4emptyLoc.toString());
    }
    */

    public void testEquals() throws Exception {
        assertEquals(true, l1.equals(l2));
    }

    public void testEqualsFalse() throws Exception {
        assertEquals(false, l1.equals(l3));
    }

}