package Cloud;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;

public class PlaceJoiner {
	
	private String filepath;
	
	// Stores <placeid, localename>
	private HashMap<String, String> idToLocale;
	
	// Stores <localename, placeID>
	private HashMap<String, String> localeToID;
	
	// Stores <neighborhoodname, localeID>
	private HashMap<String, String> neighborhoodLocales;
	
	// Stores <localeID, countryName>
	private HashMap<String, String> localeIDToCountry;
	
	private String[] tempResult;

	private boolean isReady = false;
	
	public PlaceJoiner(String path, FSDataInputStream fs)
	{
		filepath = path;
		idToLocale = new HashMap<String, String>();
		localeToID = new HashMap<String, String>();
		localeIDToCountry = new HashMap<String, String>();
		neighborhoodLocales = new HashMap<String, String>();
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
					idToLocale.put(placeID, placeName);
					localeIDToCountry.put(placeID, country);
				}
				else if (placeType.equals("22"))
				{
					/*
					s = s.replace(",", "");
					String[] nhoodValues = s.split(" ");
					String locale = nhoodValues[1];			// Assume that the 2nd value is a locale - might not be, but will test.
					String tagCountry = nhoodValues[nhoodValues.length - 1];	// Assume the last value is the country
					*/
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
