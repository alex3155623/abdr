package test;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import monitor.Monitor;
import monitor.MonitorInterface;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import application.Application;
import db.KVDB;
import db.KVDBInterface;

public class TestApplication {
	static String storeName = "kvstore";
	static String hostName = "ari-31-201-07";
	static int hostPort = 31500;
	static int rmiPort = 55000;
	static Map<Integer, KVDBInterface> kvdbs = new HashMap<Integer, KVDBInterface>();
	static Map<Integer, MonitorInterface> monitors = new HashMap<Integer, MonitorInterface>();
	static List<Application> applications = new ArrayList<Application>();
	static int nbProfilePerKVDB = 5;
	static int nbKVDB = 2;
	static int nbMonitor = 2;
	
	
	private static MonitorInterface getRemoteMonitor(String monitorService, String hostName, int port) {
		MonitorInterface targetMonitor = null;
		try {
		Registry myRegistry = LocateRegistry.getRegistry(hostName, port);
		
		targetMonitor = (MonitorInterface) myRegistry.lookup(monitorService);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
		return targetMonitor;
	}
	
	private static KVDBInterface getRemoteKVDB(String kvdbService, String hostName, int port) {
		KVDBInterface targetKVDB = null;
		try {
		Registry myRegistry = LocateRegistry.getRegistry(hostName, port);
		
		targetKVDB = (KVDBInterface) myRegistry.lookup(kvdbService);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
		return targetKVDB;
	}
	
	
	@BeforeClass
	public static void onlyOnce() throws RemoteException {
	    int temp = hostPort;
	    
	    //create all kvdbs
	    String kvdbServiceId = "";
	    KVDBInterface currentKVDB= null;
	    for (int i = 0; i < nbProfilePerKVDB * nbKVDB; i++) {
	    	if (i % nbProfilePerKVDB == 0) {
	    		
		    	KVDB.startKVDB(hostName, rmiPort, i, storeName, hostName, temp + "");
		    	kvdbServiceId = "KVDB" + i;
		    	currentKVDB = getRemoteKVDB(kvdbServiceId, hostName, rmiPort);
		    	rmiPort++;
		    	temp += 2;
	    	}
			kvdbs.put(i, currentKVDB);
	    }
	    
	    //create all monitors
	    String monitorServiceId = "";
	    MonitorInterface currentMonitor = null;
	    Monitor.startMonitor(kvdbs, 0, hostName, rmiPort);
		monitorServiceId = "monitor" + 0;
		currentMonitor = getRemoteMonitor(monitorServiceId, hostName, rmiPort);
		rmiPort++;
	    for (int i = 0; i < nbProfilePerKVDB * nbMonitor; i++) {
	    	monitors.put(i, currentMonitor);
	    }
	    
	    //init neighbour of kvdbs + monitors
	    for (int i = 0; i < nbKVDB; i++) {
	    	int fakeId = (i * nbProfilePerKVDB) + (kvdbs.size() * nbProfilePerKVDB);
			try {
				kvdbs.get(i * nbProfilePerKVDB).setLeftKVDB(kvdbs.get((fakeId - nbProfilePerKVDB) % (kvdbs.size() * nbProfilePerKVDB)));
				kvdbs.get(i * nbProfilePerKVDB).setRightKVDB(kvdbs.get((fakeId + nbProfilePerKVDB) % (kvdbs.size() * nbProfilePerKVDB)));
				kvdbs.get(i * nbProfilePerKVDB).setMonitors(monitors);
			} catch (RemoteException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    }
	    
	    Set<Integer> keys = kvdbs.keySet();
	    for (Integer kvdbIndex : keys) {
	    	//kvdbs.get(kvdbIndex).startDB();
	    }
	    
//	    //init applications
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
//	    applications.add(new Application(9, targetProfiles9, monitors, 10, 9000000));
//	    applications.add(new Application(10, targetProfiles10, monitors, 10, 10000000));
	}
	
	@AfterClass
	public static void after() {
		Set<Integer> keys = kvdbs.keySet();
		for (Integer dbIndex : keys) {
			//kvdbs.get(dbIndex).closeDB();
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
		targetProfiles.add(9);
		
		List<Integer> targetProfiles2 = new ArrayList<Integer>();
		targetProfiles2.add(5);
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
