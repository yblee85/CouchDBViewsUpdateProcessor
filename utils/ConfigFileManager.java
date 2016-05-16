package utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class ConfigFileManager {

	public static Map<String, Object> readConfigFileIntoMap(String path) {
		BufferedReader reader;
		Map<String, Object> mapFile = new TreeMap<String, Object>();
		
		try {
			reader = new BufferedReader( new FileReader (path));
			String line = null;
			String currentKey = "";
			String strContents = "";
			while( ( line = reader.readLine() ) != null ) {
				line = line.trim();
				
				if(line.startsWith("[") && line.endsWith("]")) {
					
					// transform content and put with currentKey
					if(!currentKey.isEmpty() && !strContents.isEmpty()) {
						strContents = strContents.trim();
						mapFile.put(currentKey, strContents);
					}
					
					// after transform content and put into a map, clean content for next part
					strContents = "";
					currentKey = line.substring(1, line.length()-1);
				} else {
					strContents = strContents.concat(line + System.getProperty("line.separator"));
				}
			}
			
			// last one transform content and put with currentKey
			if(!currentKey.isEmpty() && !strContents.isEmpty()) {
				strContents = strContents.trim();
				mapFile.put(currentKey, strContents);
			}
			
			reader.close();
		} catch (FileNotFoundException e) {
			mapFile = null;
		} catch (IOException e) {
			e.printStackTrace();
			mapFile = null;
		} catch(Exception e) {
			e.printStackTrace();
			mapFile = null;
		}
		
		return mapFile;
	}
	
	public static boolean writeMapIntoConfigFile(String path, Map<String, Object> map) {
		File f = new File(path);
		String newLine = System.getProperty("line.separator");
		
		try {
			if(map != null && !map.isEmpty()) {
				BufferedWriter out = new BufferedWriter(new FileWriter(f));
				for(String strKey : map.keySet()) {
					String val = map.get(strKey).toString().trim();
					out.write("[" + strKey.trim() + "]" + newLine);
					out.write(val + newLine);
				}
				out.close();
				return true;
			} else {
				return true;
			}
		} catch(Exception e) {
			e.printStackTrace();
			return false;
		}
	}
	
}
