package utils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.TreeMap;

import org.codehaus.jackson.JsonNode;

public class ConverterJsonNodeToMap {
	public static ArrayList<Object> convertJsonnodeToArrayList(JsonNode node) {
    	ArrayList<Object> list = new ArrayList<Object>();
    	Iterator<JsonNode> itr = node.getElements();
    	
    	while(itr.hasNext()) {
    		JsonNode el = itr.next();
    		
    		if(el.isObject()) {
				list.add(convertJsonnodeToTreeMapComplete(el));
			} else if(el.isBoolean())  {
				list.add(el.getBooleanValue());
			} else if(el.isTextual())  {
				list.add(el.getTextValue());
			} else if(el.isBigDecimal())  {
				list.add(el.getDecimalValue());
			} else if(el.isInt()) {
				list.add(el.getIntValue());
			} else if(el.isDouble()) {
				list.add(el.getDoubleValue());
			}
			else if(el.isArray()) {
				list.add(convertJsonnodeToArrayList(el));
			} 
			else {
				list.add(el.getTextValue());
			}
    	}
    	
    	return list;
    }
	
    // XXX, NOTICE : do not use this method yet, this is supposed to convert complex json node but it's not done yet.
    // it seems work, yay
    public static TreeMap<String, Object> convertJsonnodeToTreeMapComplete(JsonNode node) {
    	Iterator<String> itr = node.getFieldNames();
		TreeMap<String,Object> doc = new TreeMap<String,Object>();
		while(itr.hasNext()) {
			String field = itr.next();
			JsonNode el = node.path(field);
			if(el.isObject()) {
				doc.put(field, convertJsonnodeToTreeMapComplete(el));
			} else if(el.isBoolean())  {
				doc.put(field, el.getBooleanValue());
			} else if(el.isTextual())  {
				doc.put(field, el.getTextValue());
			} else if(el.isBigDecimal())  {
				doc.put(field, el.getDecimalValue());
			} else if(el.isInt()) {
				doc.put(field, el.getIntValue());
			} else if(el.isDouble()) {
				doc.put(field, el.getDoubleValue());
			} 
			else if(el.isArray()) {
				doc.put(field, convertJsonnodeToArrayList(el));
			} 
			else {
				doc.put(field, el.getTextValue());
			}
		}
		return doc;
    }
}
