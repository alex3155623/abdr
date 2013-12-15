import java.util.ArrayList;
import java.util.List;

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
	    //temp = hostPort;
	    //for (int i = 0; i < 2; i++) {
	    //	System.out.println("DB = " + temp);
	    System.out.println("nb db = " + kvdbs.size());
			kvdbs.get(0).printDB();
		//	temp += 2;
	    //}
			kvdbs.get(0).closeDB();
			kvdbs.get(1).closeDB();
		    
	    
	    //TODO create monitors
	    
	    //TODO create applications
	    
	    //execute benchmark
	}
}
