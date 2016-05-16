package apps;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.ektorp.CouchDbConnector;
import org.ektorp.CouchDbInstance;
import org.ektorp.ViewQuery;
import org.ektorp.ViewResult;
import org.ektorp.http.HttpClient;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbConnector;
import org.ektorp.impl.StdCouchDbInstance;

import utils.ConfigFileManager;
import utils.ConverterJsonNodeToMap;

public class GeneralTouchViewWithMaxProcessor {

	int MAX_THREAD_SLEEP_COUNT = 30;
	int THREAD_SLEEP_TIME = 2000;
	String DEFAULT_POINT_FILE_NAME = "processed_point";
	int current_thread_sleep_count = 0;
	
	
	public GeneralTouchViewWithMaxProcessor(String[] args) {
		String address = args[0];
		String id = args[1];
		String password = args[2];
		BigInteger timeout = new BigInteger(args[3]);
		String location = args[4];
		BigInteger maxProcess = new BigInteger(args[5]);
		current_thread_sleep_count = 0;
		
		// this makes views up-to-date
		CouchDbInstance dbInstance;
		HttpClient httpLocal;
		
		try {
			httpLocal = new StdHttpClient.Builder().connectionTimeout(timeout.intValue()).socketTimeout(timeout.intValue()).url(address).username(id).password(password).build();
			dbInstance = new StdCouchDbInstance(httpLocal);

			System.out.println("start touch views");
			
			// no timer, general
			touch_view(httpLocal,dbInstance,location, maxProcess);
			
			System.out.println("end touch views " + (new Date()).toString());
			System.out.println("timeout : " + timeout.toString());
			
			httpLocal.shutdown();
			
			System.out.println("shutdown connection.. ");
		} catch (MalformedURLException e1) {
			e1.printStackTrace();
		}
	}
	
	private void touch_view(HttpClient httpLocal, CouchDbInstance dbInstance, String location, BigInteger maxProcess) {
		// random
		// decide whether if this goes in order or reverse
		Random rand = new Random(); 
		int rndNum = rand.nextInt(50); // 0 ~ 49
		boolean isGoReverse = false;
		isGoReverse = ((rndNum&1) == 0);	// check if it's even number
		String iniFileName = DEFAULT_POINT_FILE_NAME;
		if(isGoReverse) 
			iniFileName = iniFileName.concat("_reverse");
		String iniFilePath = iniFileName.concat(".ini");
		
		// check info file
		File iniFile = new File(iniFilePath);
		if(!iniFile.exists()) {
			try {
				iniFile.createNewFile();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
		
		// default start point info
		String start_db_name ="";
		String start_design_name = "";
		String start_view_name ="";
		
		// read config file
		Map<String, Object> mapINI = null;
		try {
			mapINI = ConfigFileManager.readConfigFileIntoMap(iniFilePath);
			String tmp = (mapINI.get("start_db_name")==null?"":mapINI.get("start_db_name").toString());
			start_db_name = tmp;
			
			tmp = (mapINI.get("start_design_name")==null?"":mapINI.get("start_design_name").toString());
			start_design_name = tmp;
			
			tmp = (mapINI.get("start_view_name")==null?"":mapINI.get("start_view_name").toString());
			start_view_name = tmp;
			
		} catch(Exception e2) {
			mapINI = new HashMap<String, Object>();
			e2.printStackTrace();
		}
		
		
		
		// read file from location and initialize map
		String data = readFile(location);
		String[] lines = data.split("\n");
		HashMap<String, HashMap<String, ArrayList<String>>> dbMap = new HashMap<String, HashMap<String, ArrayList<String>>>();
		
		for(int i=0; i<lines.length; i++) {
			String[] info = lines[i].split(",");
			String dbname = info[0].trim();
			String designname = info[1].trim();
			String viewname = info[2].trim();
			
			HashMap<String, ArrayList<String>> designMap = dbMap.get(dbname);
			if(designMap == null) {
				designMap = new HashMap<String, ArrayList<String>>();
				dbMap.put(dbname, designMap);
			}
				
			ArrayList<String> viewnames = designMap.get(designname);
			
			if(viewnames == null) {
				viewnames = new ArrayList<String>();
				designMap.put(designname, viewnames);
			}
			
			if(!viewnames.contains(viewname)) {
				viewnames.add(viewname);
			}
		}
		
		System.out.println("here we go....");
		for(Map.Entry<String, HashMap<String, ArrayList<String>>> entry : dbMap.entrySet()){
			String dbn = entry.getKey();
			HashMap<String, ArrayList<String>> design_views = dbMap.get(dbn);
			for(Map.Entry<String, ArrayList<String>> views : design_views.entrySet()) {
				String designn = views.getKey();
				List<String> liViews = design_views.get(designn);
				for(String viewn : liViews) {
					System.out.println("db : " + dbn + ", design : " + designn + ", view : " + viewn);
				}
			}
		}
		System.out.println("there you are.....");
		
		int numOfTask = getNumberOfTask(httpLocal, "indexer");
		if(numOfTask == -1 || numOfTask >= maxProcess.intValue()) return ;
		
		// fetch all dbs
		List<String> allDbs = dbInstance.getAllDatabases();
		
		if(isGoReverse) {
			// will revert db list
			try {
				Collections.reverse(allDbs);
			} catch(Exception e) {
				// reverse error
				System.out.println("reverse list error");
			}
		}
		
		ArrayList<String> listTransactionsDbs = new ArrayList<String>(); //dbInstance.getAllDatabases();
		ArrayList<String> listCashedoutTransactionsDbs = new ArrayList<String>(); //dbInstance.getAllDatabases();
		Iterator<String> itr_dbname = allDbs.iterator();
		while(itr_dbname.hasNext()) {
			String dbName = itr_dbname.next();
			if(dbName.contains("transactions") && !dbName.contains("designs")) {
				if(!dbName.contains("cashedout")) {
					listTransactionsDbs.add(dbName);
				} else {
					listCashedoutTransactionsDbs.add(dbName);
				}
			}
		}
		
		for(Map.Entry<String, HashMap<String, ArrayList<String>>> entry : dbMap.entrySet()){
		    String dbname = entry.getKey();
		    
		    if(dbname.equals("transactions")) {
    			Iterator<String> iter = listTransactionsDbs.iterator();
    			while(iter.hasNext()) {
    				String db_name = iter.next();
    				CouchDbConnector db = new StdCouchDbConnector(db_name, dbInstance);
    				
    				HashMap<String, ArrayList<String>> value = entry.getValue();
    			    for(Map.Entry<String, ArrayList<String>> innerentry : value.entrySet()) {
    			    	String designname = innerentry.getKey();
    			    	ArrayList<String> innervalues = innerentry.getValue();
    			    	
    			    	Iterator<String> itr = innervalues.iterator();
    			    	while(itr.hasNext()) {
    			    		numOfTask = getNumberOfTask(httpLocal, "indexer");
    			    		if(numOfTask == -1 || numOfTask >= maxProcess.intValue()) {
    			    			if(numOfTask == -1) {
    			    				// before return save ini for next run
    			    				try {
    			    					mapINI.put("start_db_name", db_name);
        			    				mapINI.put("start_design_name", designname);
        			    				mapINI.put("start_view_name", itr.next());
        			    				ConfigFileManager.writeMapIntoConfigFile(iniFilePath, mapINI);
    			    				} catch(Exception e2) {
    			    					e2.printStackTrace();
    			    				}
    			    				return;
    			    			} 
    			    			else {
    			    				while(numOfTask >= maxProcess.intValue() && current_thread_sleep_count<MAX_THREAD_SLEEP_COUNT) {
    			    					try {
    			    						current_thread_sleep_count++;
    			    						System.out.println("will sleep ...");
											Thread.sleep(THREAD_SLEEP_TIME);
											System.out.println("wake up!");
										} catch (InterruptedException e) {
											e.printStackTrace();
										}
    			    					numOfTask = getNumberOfTask(httpLocal, "indexer");
    			    				}
    			    				if(numOfTask == -1 || current_thread_sleep_count>=MAX_THREAD_SLEEP_COUNT) {
    			    					// before return save ini for next run
        			    				try {
        			    					mapINI.put("start_db_name", db_name);
            			    				mapINI.put("start_design_name", designname);
            			    				mapINI.put("start_view_name", itr.next());
            			    				ConfigFileManager.writeMapIntoConfigFile(iniFilePath, mapINI);
        			    				} catch(Exception e2) {
        			    					e2.printStackTrace();
        			    				}
    			    					return;
    			    				}
    			    				else current_thread_sleep_count = 0;
    			    			}
    			    		}
//    			    		if(numOfTask == -1 || numOfTask >= maxProcess.intValue()) return ;
    			    		
    			    		String viewname = itr.next();
    			    		if(start_db_name!=null && !start_db_name.isEmpty() &&
    			    				start_design_name!=null && !start_design_name.isEmpty() &&
    			    				start_view_name!=null && !start_view_name.isEmpty() &&
    			    				( !start_db_name.equals(db_name) || !start_design_name.equals(designname) || !start_view_name.equals(viewname)) ){
    			    			// skip
    			    			continue;
    			    		} else {
    			    			// will query it so delete ini
    			    			// and from this moment, don't skip
    			    			start_db_name = "";
    			    			start_design_name = "";
    			    			start_view_name = "";
    			    			try {
    			    				iniFile.delete();
    			    			} catch(Exception e2) {
    			    				System.out.println("can't delete ini file : " + iniFileName);
    			    			}
    			    		}
    			    		
    				    	try {
    							ViewResult result = db.queryView(new ViewQuery().designDocId(designname).staleOkUpdateAfter().viewName(viewname).limit(5));
    							System.out.println("touch view done! db : " + db_name + ", design : " + designname + ", view : " + viewname);
//    							for(Row row : result.getRows()) {
//    								System.out.println(row.getValue());
//    							}
    						} catch(Exception e) {
    							e.printStackTrace();
    						}
    			    	}
    			    }
    			}
		    } else if(dbname.equals("cashedout_transactions")) {
		    	Iterator<String> iter = listCashedoutTransactionsDbs.iterator();
    			while(iter.hasNext()) {
    				String db_name = iter.next();
    				CouchDbConnector db = new StdCouchDbConnector(db_name, dbInstance);
    				
    				HashMap<String, ArrayList<String>> value = entry.getValue();
    			    for(Map.Entry<String, ArrayList<String>> innerentry : value.entrySet()) {
    			    	String designname = innerentry.getKey();
    			    	ArrayList<String> innervalues = innerentry.getValue();
    			    	
    			    	Iterator<String> itr = innervalues.iterator();
    			    	while(itr.hasNext()) {
    			    		numOfTask = getNumberOfTask(httpLocal, "indexer");
    			    		if(numOfTask == -1 || numOfTask >= maxProcess.intValue()) {
    			    			if(numOfTask == -1) {
    			    				// before return save ini for next run
    			    				try {
    			    					mapINI.put("start_db_name", db_name);
        			    				mapINI.put("start_design_name", designname);
        			    				mapINI.put("start_view_name", itr.next());
        			    				ConfigFileManager.writeMapIntoConfigFile(iniFilePath, mapINI);
    			    				} catch(Exception e2) {
    			    					e2.printStackTrace();
    			    				}
    			    				
    			    				return;
    			    			}
    			    			else {
    			    				while(numOfTask >= maxProcess.intValue() && current_thread_sleep_count<MAX_THREAD_SLEEP_COUNT) {
    			    					try {
    			    						current_thread_sleep_count++;
    			    						System.out.println("will sleep ...");
											Thread.sleep(THREAD_SLEEP_TIME);
											System.out.println("wake up!");
										} catch (InterruptedException e) {
											e.printStackTrace();
										}
    			    					numOfTask = getNumberOfTask(httpLocal, "indexer");
    			    				}
    			    				if(numOfTask == -1 || current_thread_sleep_count>=MAX_THREAD_SLEEP_COUNT) {
    			    					// before return save ini for next run
        			    				try {
        			    					mapINI.put("start_db_name", db_name);
            			    				mapINI.put("start_design_name", designname);
            			    				mapINI.put("start_view_name", itr.next());
            			    				ConfigFileManager.writeMapIntoConfigFile(iniFilePath, mapINI);
        			    				} catch(Exception e2) {
        			    					e2.printStackTrace();
        			    				}
    			    					return;
    			    				}
    			    				else current_thread_sleep_count = 0;
    			    			}
    			    		}
//    			    		if(numOfTask == -1 || numOfTask >= maxProcess.intValue()) return ;
    			    		
    			    		String viewname = itr.next();
    			    		
    			    		if(start_db_name!=null && !start_db_name.isEmpty() &&
    			    				start_design_name!=null && !start_design_name.isEmpty() &&
    			    				start_view_name!=null && !start_view_name.isEmpty() &&
    			    				( !start_db_name.equals(db_name) || !start_design_name.equals(designname) || !start_view_name.equals(viewname)) ){
    			    			// skip
    			    			continue;
    			    		} else {
    			    			// will query it so delete ini
    			    			// and from this moment, don't skip
    			    			start_db_name = "";
    			    			start_design_name = "";
    			    			start_view_name = "";
    			    			try {
    			    				iniFile.delete();
    			    			} catch(Exception e2) {
    			    				System.out.println("can't delete ini file : " + iniFileName);
    			    			}
    			    		}
    			    		
    				    	try {
    							ViewResult result = db.queryView(new ViewQuery().designDocId(designname).staleOkUpdateAfter().viewName(viewname).limit(5));
    							System.out.println("touch view done! db : " + db_name + ", design : " + designname + ", view : " + viewname);
//    							for(Row row : result.getRows()) {
//    								System.out.println(row.getValue());
//    							}
    						} catch(Exception e) {
    							e.printStackTrace();
    							System.out.println("touch view error! db : " + db_name + ", design : " + designname + ", view : " + viewname);
    						}
    			    	}
    			    }
    			}
		    } else {
		    	Iterator<String> itr_tmp_dbname = allDbs.iterator();
				while(itr_tmp_dbname.hasNext()) {
					String dbFullName = itr_tmp_dbname.next();
					if(dbFullName.contains(dbname) && !dbFullName.contains("designs")) {
						CouchDbConnector db = new StdCouchDbConnector(dbFullName, dbInstance);
				    	
				    	HashMap<String, ArrayList<String>> value = entry.getValue();
					    for(Map.Entry<String, ArrayList<String>> innerentry : value.entrySet()) {
					    	String designname = innerentry.getKey();
					    	ArrayList<String> innervalues = innerentry.getValue();
					    	
					    	Iterator<String> itr = innervalues.iterator();
					    	while(itr.hasNext()) {
					    		numOfTask = getNumberOfTask(httpLocal, "indexer");
	    			    		if(numOfTask == -1 || numOfTask >= maxProcess.intValue()) {
	    			    			if(numOfTask == -1) {
	    			    				// before return save ini for next run
	    			    				try {
	    			    					mapINI.put("start_db_name", dbFullName);
	        			    				mapINI.put("start_design_name", designname);
	        			    				mapINI.put("start_view_name", itr.next());
	        			    				ConfigFileManager.writeMapIntoConfigFile(iniFilePath, mapINI);
	    			    				} catch(Exception e2) {
	    			    					e2.printStackTrace();
	    			    				}
	    			    				return;
	    			    			}
	    			    			else {
	    			    				while(numOfTask >= maxProcess.intValue() && current_thread_sleep_count<MAX_THREAD_SLEEP_COUNT) {
	    			    					try {
	    			    						current_thread_sleep_count++;
	    			    						System.out.println("will sleep ...");
												Thread.sleep(THREAD_SLEEP_TIME);
												System.out.println("wake up!");
											} catch (InterruptedException e) {
												e.printStackTrace();
											}
	    			    					numOfTask = getNumberOfTask(httpLocal, "indexer");
	    			    				}
	    			    				if(numOfTask == -1 || current_thread_sleep_count>=MAX_THREAD_SLEEP_COUNT) {
	    			    					// before return save ini for next run
	        			    				try {
	        			    					mapINI.put("start_db_name", dbFullName);
	            			    				mapINI.put("start_design_name", designname);
	            			    				mapINI.put("start_view_name", itr.next());
	            			    				ConfigFileManager.writeMapIntoConfigFile(iniFilePath, mapINI);
	        			    				} catch(Exception e2) {
	        			    					e2.printStackTrace();
	        			    				}
	    			    					return;
	    			    				}
	    			    				else current_thread_sleep_count = 0;
	    			    			}
	    			    		}
//					    		if(numOfTask == -1 || numOfTask >= maxProcess.intValue()) return ;
					    		
					    		String viewname = itr.next();
					    		if(start_db_name!=null && !start_db_name.isEmpty() &&
	    			    				start_design_name!=null && !start_design_name.isEmpty() &&
	    			    				start_view_name!=null && !start_view_name.isEmpty() &&
	    			    				( !start_db_name.equals(dbFullName) || !start_design_name.equals(designname) || !start_view_name.equals(viewname)) ){
	    			    			// skip
	    			    			continue;
	    			    		} else {
	    			    			// will query it so delete ini
	    			    			// and from this moment, don't skip
	    			    			start_db_name = "";
	    			    			start_design_name = "";
	    			    			start_view_name = "";
	    			    			try {
	    			    				iniFile.delete();
	    			    			} catch(Exception e2) {
	    			    				System.out.println("can't delete ini file : " + iniFileName);
	    			    			}
	    			    		}
						    	try {
									ViewResult result = db.queryView(new ViewQuery().designDocId(designname).staleOkUpdateAfter().viewName(viewname).limit(5));
									System.out.println("touch view done! db : " + dbFullName + ", design : " + designname + ", view : " + viewname);
								} catch(Exception e) {
									e.printStackTrace();
								}
					    	}
					    }
					}
				}
		    	
		    }
		}
		
		// done
		// actually don't need it, cuz it deleted first time and create only when it's unusual return
//		try {
//			iniFile.delete();
//		} catch(Exception e2) {}
	}
	
	private String readFile(String location) {
		String result = "";
	    try {
	    	BufferedReader br = new BufferedReader(new FileReader(location));
	        StringBuilder sb = new StringBuilder();
	        String line = br.readLine();

	        while (line != null) {
	            sb.append(line);
	            sb.append("\n");
	            line = br.readLine();
	        }
	        br.close();
	        
	        result = sb.toString();
	    } catch(Exception e) {
	    	e.printStackTrace();
	    } 
	    
	    return result;
	}
	
	public int getNumberOfTask(HttpClient httpLocal, String taskNameIn_active_task) {
		java.io.InputStream is = null;
		try {
			is = httpLocal.get("/_active_tasks").getContent();
			
			ObjectMapper om = new ObjectMapper();
			JsonNode node = om.readValue(is, JsonNode.class);
			
			ArrayList<Object> list = ConverterJsonNodeToMap.convertJsonnodeToArrayList(node);
			Iterator<Object> itr = list.iterator();
			int numOfTask = 0;
			
			while(itr.hasNext()) {
				Object obj = itr.next();
				String type = ((TreeMap<String,Object>)obj).get("type").toString();
				if(type.equals(taskNameIn_active_task)) numOfTask++;
			}
			
			is.close();
			is=null;
			
			System.out.println("num of " + taskNameIn_active_task + " task : " + numOfTask);
			return numOfTask;
		} catch (IOException e) {
			e.printStackTrace();
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			if(is!=null) {
				try {
					is.close();
					is=null;
				} catch(IOException e) {
				}
			}
		}
		System.out.println("num of " + taskNameIn_active_task + " task : error ");
		return -1;
	}
}
