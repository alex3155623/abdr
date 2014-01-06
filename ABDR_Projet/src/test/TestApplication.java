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
	    	//kvdbs.get(kvdbIndex).startDB();
	    }
	    
	    //init applications
//	    List<Integer> targetProfiles1 = new ArrayList<Integer>();
//	    targetProfiles1.add(1);
//	    
//	    List<Integer> targetProfiles2 = new ArrayList<Integer>();
//	    targetProfiles2.add(1);
//	    
//	    List<Integer> targetProfiles3 = new ArrayList<Integer>();
//	    targetProfiles3.add(1);
//	    
//	    List<Integer> targetProfiles4 = new ArrayList<Integer>();
//	    targetProfiles4.add(1);
//	    
//	    List<Integer> targetProfiles5 = new ArrayList<Integer>();
//	    targetProfiles5.add(1);
//	    
//	    List<Integer> targetProfiles6 = new ArrayList<Integer>();
//	    targetProfiles6.add(1);
//	    
//	    List<Integer> targetProfiles7 = new ArrayList<Integer>();
//	    targetProfiles7.add(1);
//	    
//	    List<Integer> targetProfiles8 = new ArrayList<Integer>();
//	    targetProfiles8.add(1);
//	    
//	    List<Integer> targetProfiles9 = new ArrayList<Integer>();
//	    targetProfiles9.add(1);
//	    
//	    List<Integer> targetProfiles10 = new ArrayList<Integer>();
//	    targetProfiles10.add(1);
//	    
//	    
//	    applications.add(new Application(1,, targetProfiles1, monitors, 10, 1000000));
//	    applications.add(new Application(2, targetProfiles2, monitors, 10, 2000000));
//	    applications.add(new Application(3, targetProfiles3, monitors, 10, 3000000));
//	    applications.add(new Application(4, targetProfiles4, monitors, 10, 4000000));
//	    applications.add(new Application(5, targetProfiles5, monitors, 10, 5000000));
//	    applications.add(new Application(6, targetProfiles6, monitors, 10, 6000000));
//	    applications.add(new Application(7, targetProfiles7, monitors, 10, 7000000));
//	    applications.add(new Application(8, targetProfiles8, monitors, 10, 8000000));
////	    applications.add(new Application(9, targetProfiles9, monitors, 10, 9000000));
////	    applications.add(new Application(10, targetProfiles10, monitors, 10, 10000000));
	}
	
	@AfterClass
	public static void after() {
		Set<Integer> keys = kvdbs.keySet();
		for (Integer dbIndex : keys) {
			kvdbs.get(dbIndex).closeDB();
	    }
	}
	
	
	/*
	@Test
	public void testApplicationSpam() throws InterruptedException {
		
		Map<Integer, Integer> res = new HashMap<Integer, Integer>();
		List<Thread> applicationsThread = new ArrayList<Thread>();
		
		int nbApp = 5;
		List<Integer> targetProfiles = new ArrayList<Integer>();
		targetProfiles.add(1);
		
		for (int i = 0; i < nbApp; i++){
			applicationsThread.add(new Thread(new Application(i, res, targetProfiles, monitors, 10, 10000000)));
		}
		
		for (Thread t : applicationsThread) {
			t.start();
		}
		
		for (Thread t : applicationsThread) {
			t.join();
		}
		
		long total = 0;
		for (int i = 0; i < nbApp; i++) {
			total += res.get(i);
		}
		total /= nbApp;
		
		System.out.println(" !!!!!!!!!!moyenne de temps = " + total);
	}
	*/
	
	@Test
	public void testApplicationSpam2Store() throws InterruptedException {
		
		Map<Integer, Integer> res = new HashMap<Integer, Integer>();
		List<Thread> applicationsThread = new ArrayList<Thread>();
		
		int nbApp = 5;
		List<Integer> targetProfiles = new ArrayList<Integer>();
		targetProfiles.add(1);
		
		List<Integer> targetProfiles2 = new ArrayList<Integer>();
		targetProfiles2.add(9);
		
		for (int i = 0; i < nbApp; i++){
			applicationsThread.add(new Thread(new Application(i, res, targetProfiles, monitors, 10, 10000000)));
			applicationsThread.add(new Thread(new Application(nbApp + i, res, targetProfiles2, monitors, 10, 10000000)));
		}
		
		for (Thread t : applicationsThread) {
			t.start();
		}
		
		for (Thread t : applicationsThread) {
			t.join();
		}
		
		long total = 0;
		long total2 = 0;
		for (int i = 0; i < nbApp; i++) {
			total += res.get(i);
			total2 += res.get(nbApp + i);
		}
		total /= nbApp;
		total2 /= nbApp;
		
		System.out.println(" !!!!!!!!!!moyenne de temps profil 1 = " + total);
		System.out.println(" !!!!!!!!!!moyenne de temps profil 2 = " + total2);
		
//		System.out.println("++++++++++++++++0------------------");
//		kvdbs.get(0).printDB();
//		System.out.println("+++++++++++++++ end -------0------------------");
//		
//		
//		System.out.println("-------------1------------------");
//		kvdbs.get(5).printDB();
//		System.out.println("------ end -------1------------------");
	}
	
}
