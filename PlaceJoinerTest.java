package Cloud;

import junit.framework.TestCase;

/**
 * Created by Ken on 5/04/2016.
 */
public class PlaceJoinerTest extends TestCase {
    PlaceJoiner pj;
    String filepath = "place.txt";

    public void setUp() throws Exception {
        super.setUp();
        pj = new PlaceJoiner(filepath);
    }

    public void tearDown() throws Exception {
    }

    public void testIsIdForLocale() throws Exception {
        assertEquals(true, pj.IsIdForLocale("k.N1uBGbCZTIUzrt9Q"));
    }

    public void testIsIdForLocaleTwo() throws Exception {
        assertEquals(true, pj.IsIdForLocale("oEYbykyRBJsNaA"));
    }

    public void testIsIdForLocaleFalse() throws Exception {
        assertEquals(false, pj.IsIdForLocale("vZhjY8GYA5kZ5Y92gQ"));
    }

    public void testGetPlaceDataByLocaleID() throws Exception {
        String[] results = pj.GetPlaceDataByLocaleID("k.N1uBGbCZTIUzrt9Q");
        assertEquals(results[0], "United Kingdom");
        assertEquals(results[1], "Eshaness, Scotland, United Kingdom");
        assertEquals(results[2], "\t \t");
    }

}