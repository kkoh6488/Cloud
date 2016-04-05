package Cloud;

import junit.framework.TestCase;

/**
 * Created by Ken on 5/04/2016.
 */
public class LocalityKeyTest extends TestCase {
    private LocalityKey l1, l2, l3, l4emptyLoc;

    // Constructor reference
    // public LocalityKey(String placeId, String country, String locality, String neighborhood, String owner)

    public void setUp() throws Exception {
        super.setUp();
        l1 = new LocalityKey("xbxI9VGYA5oZH8tLJA", "United Kingdom", "Coventry City, United Kingdom", "\t \t", "7556490@N05");
        l2 = new LocalityKey("xbxI9VGYA5oZH8tLJA", "United Kingdom", "Coventry City, United Kingdom", "\t \t", "7556490@N05");
        l3 = new LocalityKey("qd2lIgybBZVRy1zw", "United States", "Seligman, Arizona, United States", "\t \t", "35386145@N05");
        l4emptyLoc = new LocalityKey("qd2lIgybBZVRy1zw", "United States", "\t \t", "\t \t", "35386145@N05");

    }

    public void testCompareTo() throws Exception {
        assertEquals(0, l1.compareTo(l2));
    }

    public void testToString() throws Exception {
        assertEquals("Coventry City, United Kingdom", l1.toString());
    }

    public void testNoLocaleToString() throws Exception {
        assertEquals("qd2lIgybBZVRy1zw", l4emptyLoc.toString());
    }

    public void testEquals() throws Exception {
        assertEquals(true, l1.equals(l2));
    }

    public void testEqualsFalse() throws Exception {
        assertEquals(false, l1.equals(l3));
    }

}