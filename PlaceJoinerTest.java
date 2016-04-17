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
        assertEquals("United Kingdom", results[0]);
        assertEquals( "Eshaness", results[1]);
        assertEquals("#", results[2]);
    }

    public void testNeighbourhoodHasLocale() throws Exception {
        /* Test data
        PbGNxHibCZlLO2VGzg	28580129	33.51	130.518	Suzaku 5 Chome, Dazaifu-shi, Fukuoka Prefecture, JP, Japan	22
        /Japan/Fukuoka+Prefecture/Dazaifu-shi/Suzaku+5+Chome
         */
        assertEquals(true, pj.IsIdForLocale("PbGNxHibCZlLO2VGzg"));
    }

    public void testNeighbourhoodHasCorrectLocale() throws Exception {
        assertEquals("tl._8UiYAJ21coEG", pj.GetLocaleIDForNeighbourhoodID("PbGNxHibCZlLO2VGzg"));
    }

    public void testNeighbourhoodPlaceData() throws Exception {
        String[] results = pj.GetPlaceDataByNeighborhoodId("PbGNxHibCZlLO2VGzg");
        assertEquals("Japan", results[0]);
        assertEquals("Dazaifu-shi", results[1]);
        assertEquals("Suzaku 5 Chome", results[2]);
    }

    public void testLocalePlaceData() throws Exception {
        String[] results = pj.GetPlaceDataByLocaleID("tl._8UiYAJ21coEG");
        assertEquals("Japan", results[0]);
        assertEquals("Dazaifu-shi", results[1]);
        assertEquals("#", results[2]);
    }

    public void testFakeLocaleData() throws Exception {
        String[] results = pj.GetPlaceDataByLocaleID("placeID-LocA");
        assertEquals("CountryA", results[0]);
        assertEquals("LocaleA", results[1]);
        assertEquals("#", results[2]);
    }

    public void testFakeNeighbourhoodData() throws Exception {
        String[] results = pj.GetPlaceDataByNeighborhoodId("placeID-NeighA");
        assertEquals("CountryA", results[0]);
        assertEquals("LocaleA", results[1]);
        assertEquals("NeighA", results[2]);
    }

    public void testFakeLocaleDataB() throws Exception {
        String[] results = pj.GetPlaceDataByLocaleID("placeID-LocB");
        assertEquals("CountryB", results[0]);
        assertEquals("LocaleB", results[1]);
        assertEquals("#", results[2]);
    }

    public void testFakeLocaleDataTwo() throws Exception {
        String[] results = pj.GetPlaceDataByLocaleID("placeID-Loc4-6");
        assertEquals("Country4", results[0]);
        assertEquals("Locale4-6", results[1]);
        assertEquals("#", results[2]);
    }

}