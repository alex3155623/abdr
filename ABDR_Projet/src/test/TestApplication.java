package test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import monitor.Monitor;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import application.Application;
import db.KVDB;

public class TestApplication {
	static String storeName = "kvstore";
	static String hostName = "ari-31-201-07";
	static int hostPort = 31500;
	static Map<Integer, KVDB> kvdbs = new HashMap<Integer, KVDB>();
	static Map<Integer, Monitor> monitors = new HashMap<Integer, Monitor>();
	static List<Application> applications = new ArrayList<Application>();
	static int nbProfilePerKVDB = 5;
	
	@BeforeClass
	public static void onlyOnce() {
		//create DBs
	    int temp = hostPort;
	    
	    
	    List<KVDB> tempList = new ArrayList<KVDB>();
	    for (int i = 0; i < 3; i++) {
	    	KVDB db = new KVDB(i * nbProfilePerKVDB, storeName, hostName, new Integer(temp).toString(), monitors);
			kvdbs.put(i * nbProfilePerKVDB, db);
			tempList.add(db);
			temp += 2;
	    }
	    
	    //init neighbour
	    for (int i = 0; i < 3; i++) {
	    	int fakeId = (i * nbProfilePerKVDB) + (kvdbs.size() * nbProfilePerKVDB);
			kvdbs.get(i * nbProfilePerKVDB).setLeftKVDB(kvdbs.get((fakeId - nbProfilePerKVDB) % (kvdbs.size() * nbProfilePerKVDB)));
			kvdbs.get(i * nbProfilePerKVDB).setRightKVDB(kvdbs.get((fakeId + nbProfilePerKVDB) % (kvdbs.size() * nbProfilePerKVDB)));
	    }
	    
	    
	    //init monitors
	    for (int i = 0; i < nbProfilePerKVDB * 3; i++) {
	    	monitors.put(i, new Monitor(tempList, 0));
	    }
	    
	    Set<Integer> keys = kvdbs.keySet();
	    for (Integer kvdbIndex : keys) {
	    	kvdbs.get(kvdbIndex).startDB();
	    }
	    
	    //init applications
	    List<Integer> targetProfiles1 = new ArrayList<Integer>();
	    targetProfiles1.add(1);
	    
	    List<Integer> targetProfiles2 = new ArrayList<Integer>();
	    targetProfiles2.add(1);
	    
	    List<Integer> targetProfiles3 = new ArrayList<Integer>();
	    targetProfiles3.add(1);
	    
	    List<Integer> targetProfiles4 = new ArrayList<Integer>();
	    targetProfiles4.add(1);
	    
	    List<Integer> targetProfiles5 = new ArrayList<Integer>();
	    targetProfiles5.add(1);
	    
	    List<Integer> targetProfiles6 = new ArrayList<Integer>();
	    targetProfiles6.add(1);
	    
	    List<Integer> targetProfiles7 = new ArrayList<Integer>();
	    targetProfiles7.add(1);
	    
	    List<Integer> targetProfiles8 = new ArrayList<Integer>();
	    targetProfiles8.add(1);
	    
	    List<Integer> targetProfiles9 = new ArrayList<Integer>();
	    targetProfiles9.add(1);
	    
	    List<Integer> targetProfiles10 = new ArrayList<Integer>();
	    targetProfiles10.add(1);
	    
	    
	    applications.add(new Application(targetProfiles1, monitors, 10, 100000));
	    //applications.add(new Application(targetProfiles2, monitors, 10, 200000));
//	    applications.add(new Application(targetProfiles3, monitors, 10, 300000));
//	    applications.add(new Application(targetProfiles4, monitors, 10, 400000));
//	    applications.add(new Application(targetProfiles5, monitors, 10, 500000));
//	    applications.add(new Application(targetProfiles6, monitors, 10, 600000));
//	    applications.add(new Application(targetProfiles7, monitors, 10, 700000));
//	    applications.add(new Application(targetProfiles8, monitors, 10, 800000));
//	    applications.add(new Application(targetProfiles9, monitors, 10, 900000));
//	    applications.add(new Application(targetProfiles10, monitors, 10, 1000000));
	}
	
	@AfterClass
	public static void after() {
		Set<Integer> keys = kvdbs.keySet();
		for (Integer dbIndex : keys) {
			kvdbs.get(dbIndex).closeDB();
	    }
	}
	
	
	
	@Test
	public void testApplicationSpam() throws InterruptedException {
		
		
		List<Thread> applicationsThread = new ArrayList<Thread>();
		for (Application app : applications) {
			applicationsThread.add(new Thread(app));
		}
		
		for (Thread t : applicationsThread) {
			t.start();
		}
		
		for (Thread t : applicationsThread) {
			t.join();
		}
		/*System.out.println("++++++++++++++++0------------------");
		kvdbs.get(0).printDB();
		System.out.println("+++++++++++++++ end -------0------------------");
		
		
		System.out.println("-------------1------------------");
		kvdbs.get(5).printDB();
		System.out.println("------ end -------1------------------");*/
	}
	
}
