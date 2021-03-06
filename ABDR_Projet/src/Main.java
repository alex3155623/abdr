import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;

import application.Application;
import monitor.Monitor;
import transaction.Data;
import db.KVDB;
import db.KVDBInterface;


public class Main {
	
	public static void main(String[]args) throws RemoteException {
		String storeName = "kvstore";
	    String hostName = "ari-31-201-02";
	    int hostPort = 5002;
	    
	    List<KVDBInterface> kvdbs = new ArrayList<KVDBInterface>();
		
	    //create DBs
	    int temp = hostPort;
	    for (int i = 0; i < 2; i++) {
			//kvdbs.add(new KVDB(i, storeName, hostName, new Integer(temp).toString(), null));
			temp += 2;
	    }
	    
	    //make db knowing each other
	    /*for (int i = 0; i < 2; i++) {
	    	for (KVDB db : kvdbs) {
	    		if (kvdbs.get(i) == db)
	    			continue;
	    		kvdbs.get(i).addNeighbor(db);
	    	}
	    }*/
	    

		kvdbs.get(0).closeDB();
		kvdbs.get(1).closeDB();
		
	    
		
	    //TODO create monitors
		//Monitor monitor = new Monitor(kvdbs);
	    
	    //TODO create applications
		//Application.start(monitor);
	    
	    //execute benchmark
	}
}
