package Cloud;

import org.apache.hadoop.fs.FSDataInputStream;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;

public class PlaceJoiner {
	
	private String filepath;
	
	// Stores <placeid, localename>
	private HashMap<String, String> idToLocale;
	
	// Stores <localename, placeID>
	private HashMap<PlacePair, String> localeToID;
	
	// Stores <neighborhoodname, localeID>
	private HashMap<String, String> neighborhoodToPlace;

	private HashMap<String, String> neighborhoodIDToName;
	
	// Stores <localeID, countryName>
	private HashMap<String, String> localeIDToCountry;
	
	private String[] tempResult;

	private boolean isReady = false;

	private PlacePair placePair;
	
	public PlaceJoiner(String path, FSDataInputStream fs)
	{
		filepath = path;
		idToLocale = new HashMap<String, String>();
		localeToID = new HashMap<PlacePair, String>();
		localeIDToCountry = new HashMap<String, String>();
		neighborhoodToPlace = new HashMap<String, String>();
		tempResult = new String[3];
		LoadPlacesIntoMemory(fs);
	}
	
	/* Read each line of the places file and load locales and neighborhoods into memory.
	 * Locality have type = 7; Neighborhoods have type = 22.
	 * We need to identify the locale for each neighborhood as well.
	 */
	private void LoadPlacesIntoMemory(FSDataInputStream fs)
	{
		BufferedReader br;
		br = new BufferedReader(new InputStreamReader(fs));
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
					placePair = new PlacePair(placeName, country);
					idToLocale.put(placeID, placeName);
					localeIDToCountry.put(placeID, country);
					localeToID.put(placePair, placeID);
				}
				else if (placeType.equals("22"))
				{
					// Try to map neighbourhoods to their locale ID using the place name.
					// Format is : Suzaku 5 Chome, Dazaifu-shi, Fukuoka Prefecture, JP, Japan
					// Problem!
					// JP, Japan -> this might cause problems mapping to a locale

					int firstcomma = placeName.indexOf(',');
					String neighbourhood = placeName.substring(0, firstcomma);
					String locale = placeName.substring(firstcomma + 2);
					placePair = new PlacePair(locale, country);
					/*
					s = s.replace(",", "");
					String[] nhoodValues = s.split(" ");
					String locale = nhoodValues[1];										// Assume that the 2nd value is a locale - might not be, but will test.
					String tagCountry = nhoodValues[nhoodValues.length - 1];			// Assume the last value is the country
					placePair = new Pair<String, String>(locale, tagCountry);
					*/
					if (localeToID.containsKey(placePair))
					{
						neighborhoodToPlace.put(placeID, localeToID.get(placePair));
						neighborhoodIDToName.put(placeID, placeName);
					}
					else	// Create a new locale
					{
						localeToID.put(placePair, placeID);
						neighborhoodToPlace.put(placeID, placeID);
						neighborhoodIDToName.put(placeID, placeName);
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
		return (idToLocale.containsKey(id));
	}

	/* Whether the given ID belongs to a neighbourhood which was successfully
	 * mapped to a locale.
	 */
	public boolean IsIdForKnownNeighborhood(String id)
	{
		return neighborhoodToPlace.containsKey(id);
	}

	// Return [countryname, localename, neighborhoodname]
	public String[] GetPlaceDataByNeighborhoodId(String neighborhoodId)
	{
		String tempPlaceId = neighborhoodToPlace.get(neighborhoodId);
		tempResult[0] = localeIDToCountry.get(tempPlaceId);
		tempResult[1] = idToLocale.get(tempPlaceId);
		tempResult[2] = neighborhoodIDToName.get(neighborhoodId);
		return tempResult;
	}

	/* Gets the place data for a given placeid.
	 * 
	 */
	public String[] GetPlaceDataByLocaleID(String id)
	{
		// Return [countryname, localename, neighborhoodname]
		tempResult[0] = localeIDToCountry.get(id);
		tempResult[1] = idToLocale.get(id);
		tempResult[2] = "\t \t";
		return tempResult;
	}
	
	public String[] GetPlaceDataByTag(String tags)
	{
		return null;
	}

	public int GetLocaleCount()
	{
		return idToLocale.size();
	}

	public Iterator<String> GetLocales()
	{
		return idToLocale.values().iterator();
	}
}
