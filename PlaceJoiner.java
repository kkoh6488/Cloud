package Cloud;

import org.apache.hadoop.fs.FSDataInputStream;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;

public class PlaceJoiner {
	
	private String filepath;
	
	// Stores <placeid, localename>
	private HashMap<String, String> idToLocaleName;
	
	// Stores <localename, placeID>
	private HashMap<PlacePair, String> localePairToID;
	
	// Stores <neighborhoodname, localeID>
	private HashMap<String, PlacePair> neighborhoodIDToPlacePair;

	private HashMap<String, String> neighborhoodIDToNBName;
	
	// Stores <localeID, countryName>
	private HashMap<String, String> localeIDToCountry;
	
	private String[] tempResult;

	private boolean isReady = false;

	private PlacePair placePair;
	
	public PlaceJoiner(FSDataInputStream fs) {
		InitialiseStructs();
		LoadPlacesIntoMemory(fs);
	}

	public PlaceJoiner(String path) {
		filepath = path;
		InitialiseStructs();
		try {
			FileReader fr = new FileReader(filepath);
			LoadPlacesIntoMemory(fr);
		}
		catch (FileNotFoundException e){
			System.out.println("Places file was not found");
		}
	}

	void InitialiseStructs()
	{
		idToLocaleName = new HashMap<String, String>();
		localePairToID = new HashMap<PlacePair, String>();
		localeIDToCountry = new HashMap<String, String>();
		neighborhoodIDToPlacePair = new HashMap<String, PlacePair>();
		neighborhoodIDToNBName = new HashMap<String, String>();
		tempResult = new String[3];
	}
	
	/* Read each line of the places file and load locales and neighborhoods into memory.
	 * Locality have type = 7; Neighborhoods have type = 22.
	 * We need to identify the locale for each neighborhood as well.
	 */
	private void LoadPlacesIntoMemory(FSDataInputStream fs)
	{
		BufferedReader br = new BufferedReader(new InputStreamReader(fs));
		String s;
		try
		{
			while ((s = br.readLine()) != null)
			{
				String[] values = s.split("\t");
				String placeID = values[0];
				String placeName = values[4];
				String placeType = values[5];
				String country = placeName.substring(placeName.lastIndexOf(",") + 2);
				if (values.length < 7)
				{
					continue;
				}
				if (placeType.equals("7"))
				{
					String locale = placeName.substring(0, placeName.indexOf(','));
					placePair = new PlacePair(locale, country);
					//if (!localePairToID.containsKey(placePair)) {
					idToLocaleName.put(placeID, locale);
					localeIDToCountry.put(placeID, country);
					localePairToID.put(placePair, placeID);
					//}
				}
				else if (placeType.equals("22"))
				{
					// Try to map neighbourhoods to their locale ID using the place name.
					// Format is : Suzaku 5 Chome, Dazaifu-shi, Fukuoka Prefecture, JP, Japan
					// Problem!
					// JP, Japan -> this might cause problems mapping to a locale

					int firstcomma = placeName.indexOf(',');
					int secondcomma = placeName.indexOf(',', firstcomma + 2);
					String neighbourhood = placeName.substring(0, firstcomma);
					String locale = placeName.substring(firstcomma + 2, secondcomma);
					placePair = new PlacePair(locale, country);
					/*
					s = s.replace(",", "");
					String[] nhoodValues = s.split(" ");
					String locale = nhoodValues[1];										// Assume that the 2nd value is a locale - might not be, but will test.
					String tagCountry = nhoodValues[nhoodValues.length - 1];			// Assume the last value is the country
					placePair = new Pair<String, String>(locale, tagCountry);
					*/
					if (localePairToID.containsKey(placePair))
					{
						neighborhoodIDToPlacePair.put(placeID, placePair);	// Want to map neighborhood ID to a locale ID for later use
						neighborhoodIDToNBName.put(placeID, neighbourhood);					// Map this ID to a neighborhood name for later use
					}
					else	// This neighborhood doesn't have an existing locale - make a new one
					{
						// Making a new locale
						localePairToID.put(placePair, placeID);
						localeIDToCountry.put(placeID, country);
						idToLocaleName.put(placeID, locale);

						// Now enter in neighbourhood
						neighborhoodIDToPlacePair.put(placeID, placePair);
						neighborhoodIDToNBName.put(placeID, neighbourhood);
					}
				}
			}
			br.close();
			isReady = true;
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}

	private void LoadPlacesIntoMemory(FileReader fr)
	{
		BufferedReader br;
		br = new BufferedReader(fr);
		String s;
		try
		{
			while ((s = br.readLine()) != null)
			{
				String[] values = s.split("\t");
				String placeID = values[0];
				String placeName = values[4];
				String placeType = values[5];
				String country = placeName.substring(placeName.lastIndexOf(",") + 2);
				if (values.length < 7)
				{
					continue;
				}
				if (placeType.equals("7"))
				{
					String locale = placeName.substring(0, placeName.indexOf(','));
					placePair = new PlacePair(locale, country);
					//if (!localePairToID.containsKey(placePair)) {
					idToLocaleName.put(placeID, locale);
					localeIDToCountry.put(placeID, country);
					localePairToID.put(placePair, placeID);
					//}
				}
				else if (placeType.equals("22"))
				{
					// Try to map neighbourhoods to their locale ID using the place name.
					// Format is : Suzaku 5 Chome, Dazaifu-shi, Fukuoka Prefecture, JP, Japan
					// Problem!
					// JP, Japan -> this might cause problems mapping to a locale

					int firstcomma = placeName.indexOf(',');
					int secondcomma = placeName.indexOf(',', firstcomma + 2);
					String neighbourhood = placeName.substring(0, firstcomma);
					String locale = placeName.substring(firstcomma + 2, secondcomma);
					placePair = new PlacePair(locale, country);
					/*
					s = s.replace(",", "");
					String[] nhoodValues = s.split(" ");
					String locale = nhoodValues[1];										// Assume that the 2nd value is a locale - might not be, but will test.
					String tagCountry = nhoodValues[nhoodValues.length - 1];			// Assume the last value is the country
					placePair = new Pair<String, String>(locale, tagCountry);
					*/
					if (localePairToID.containsKey(placePair))
					{
						neighborhoodIDToPlacePair.put(placeID, placePair);	// Want to map neighborhood ID to a locale ID for later use
						neighborhoodIDToNBName.put(placeID, neighbourhood);					// Map this ID to a neighborhood name for later use
					}
					else	// This neighborhood doesn't have an existing locale - make a new one
					{
						// Making a new locale
						localePairToID.put(placePair, placeID);
						localeIDToCountry.put(placeID, country);
						idToLocaleName.put(placeID, locale);

						// Now enter in neighbourhood
						neighborhoodIDToPlacePair.put(placeID, placePair);
						neighborhoodIDToNBName.put(placeID, neighbourhood);
					}
				}
			}
			br.close();
			isReady = true;
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}

	public boolean IsReady()
	{
		return isReady;
	}

	/* Checks if the placeid associated with a photo is for a locale.
	 * (ie no neighborhood data)
	 */
	public boolean IsIdForLocale(String id)
	{
		return (idToLocaleName.containsKey(id));
	}

	/* Whether the given ID belongs to a neighbourhood which was successfully
	 * mapped to a locale.
	 */
	public boolean IsIdForKnownNeighborhood(String id)
	{
		return neighborhoodIDToPlacePair.containsKey(id);
	}

	// Return [countryname, localename, neighborhoodname]
	public String[] GetPlaceDataByNeighborhoodId(String neighborhoodId)
	{
		PlacePair localePair = neighborhoodIDToPlacePair.get(neighborhoodId);
		String localeId = localePairToID.get(localePair);
		tempResult[0] = localeIDToCountry.get(localeId);
		tempResult[1] = idToLocaleName.get(localeId);
		tempResult[2] = neighborhoodIDToNBName.get(neighborhoodId);
		return tempResult;
	}

	public String GetLocaleIDForNeighbourhoodID(String nID) {
		PlacePair p = neighborhoodIDToPlacePair.get(nID);
		return localePairToID.get(p);
	}

	/* Gets the place data for a given placeid.
	 * 
	 */
	public String[] GetPlaceDataByLocaleID(String id)
	{
		// Return [countryname, localename, neighborhoodname]
		tempResult[0] = localeIDToCountry.get(id);
		tempResult[1] = idToLocaleName.get(id);
		tempResult[2] = "NA";
		return tempResult;
	}
	
	public String[] GetPlaceDataByTag(String tags)
	{
		return null;
	}

	public int GetLocaleCount()
	{
		return idToLocaleName.size();
	}

	public Iterator<String> GetLocales()
	{
		return idToLocaleName.values().iterator();
	}
}
