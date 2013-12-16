import java.util.ArrayList;
import java.util.List;

import application.Application;

import monitor.Monitor;

import transaction.Data;
import db.KVDB;


public class Main {
	
	
	//public static void 
	
	
	public static void main(String[]args) {
		String storeName = "kvstore";
	    String hostName = "ari-31-201-02";
	    int hostPort = 5002;
	    
	    List<KVDB> kvdbs = new ArrayList<KVDB>();
		
	    //create DBs
	    int temp = hostPort;
	    for (int i = 0; i < 2; i++) {
			kvdbs.add(new KVDB(i, storeName, hostName, new Integer(temp).toString()));
			temp += 2;
	    }
	    
	    //make db knowing each other
	    for (int i = 0; i < 2; i++) {
	    	for (KVDB db : kvdbs) {
	    		if (kvdbs.get(i) == db)
	    			continue;
	    		kvdbs.get(i).addNeighbor(db);
	    	}
	    }
	    

	    System.out.println("_______________________________debut BASE 0_______________________________");
		kvdbs.get(0).printDB();
		System.out.println("_______________________________debut BASE 1_______________________________");
		kvdbs.get(1).printDB();
	    
	    //test migration

	    
		//System.out.println("_______________________________debut BASE 0_______________________________");
		//kvdbs.get(0).printDB();
		//System.out.println("_______________________________debut BASE 1_______________________________");
		//kvdbs.get(1).printDB();

		kvdbs.get(0).closeDB();
		kvdbs.get(1).closeDB();
		
	    
		
	    //TODO create monitors
		Monitor monitor = new Monitor(kvdbs);
	    
	    //TODO create applications
		Application.start(monitor);
	    
	    //execute benchmark
	}
}
