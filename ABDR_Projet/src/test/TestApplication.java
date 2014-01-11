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
	static int nbKVDB = 3;
	static int nbKVDBPerMonitor = 3;
	static int nbMonitor = 2;
	static List<KVDBInterface> allKvdbs = new ArrayList<KVDBInterface>();
	static boolean activeLoadBalance = false;
	
	
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
		    	allKvdbs.add(currentKVDB);
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
	    	int tempId = (i * nbProfilePerKVDB);

			try {
				kvdbs.get(tempId).setLeftKVDB(kvdbs.get((tempId - nbProfilePerKVDB + (nbKVDB * nbProfilePerKVDB)) % (nbKVDB * nbProfilePerKVDB)));
				kvdbs.get(tempId).setRightKVDB(kvdbs.get((tempId + nbProfilePerKVDB + (nbKVDB * nbProfilePerKVDB)) % (nbKVDB * nbProfilePerKVDB)));
				kvdbs.get(tempId).setMonitors(monitors);
				kvdbs.get(tempId).setSelf(kvdbs.get(tempId));
//				System.out.println("kvdb " + kvdbs.get(tempId).getKVDBId() 
//						+ " left = " + kvdbs.get((tempId - nbProfilePerKVDB + (nbKVDB * nbProfilePerKVDB)) % (nbKVDB * nbProfilePerKVDB)).getKVDBId()
//						+ " right = " + kvdbs.get((tempId + nbProfilePerKVDB + (nbKVDB * nbProfilePerKVDB)) % (nbKVDB * nbProfilePerKVDB)).getKVDBId()
//						);
			} catch (RemoteException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    }
	    
	    if (activeLoadBalance) {
		    Set<Integer> keys = kvdbs.keySet();
		    for (KVDBInterface kvdb : allKvdbs) {
		    	kvdb.startLoadBalance();
		    }
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
	public static void after() throws RemoteException {		
		for (KVDBInterface kvdb :allKvdbs) {
			kvdb.closeDB();
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
	public void testApplicationSpam2Store() throws InterruptedException, RemoteException {
		
		Map<Integer, Integer> res = new HashMap<Integer, Integer>();
		List<Thread> applicationsThread = new ArrayList<Thread>();
		
		int nbApp = 1;
		
		List<Integer> targetProfiles0 = new ArrayList<Integer>();
		targetProfiles0.add(0);
		
		List<Integer> targetProfiles1 = new ArrayList<Integer>();
		targetProfiles1.add(1);
		
		List<Integer> targetProfiles2 = new ArrayList<Integer>();
		targetProfiles2.add(2);
		
		List<Integer> targetProfiles3 = new ArrayList<Integer>();
		targetProfiles3.add(3);
		
		List<Integer> targetProfiles4 = new ArrayList<Integer>();
		targetProfiles4.add(4);
		
		
		for (int i = 0; i < nbApp; i++){
			applicationsThread.add(new Thread(new Application((nbApp * 0) + i, res, targetProfiles0, monitors, 10, 10000000)));
			applicationsThread.add(new Thread(new Application((nbApp * 1) + i, res, targetProfiles1, monitors, 10, 10000000)));
			applicationsThread.add(new Thread(new Application((nbApp * 2) + i, res, targetProfiles2, monitors, 10, 10000000)));
			applicationsThread.add(new Thread(new Application((nbApp * 3) + i, res, targetProfiles3, monitors, 10, 10000000)));
			applicationsThread.add(new Thread(new Application((nbApp * 4) + i, res, targetProfiles4, monitors, 10, 10000000)));
		}
		
		for (Thread t : applicationsThread) {
			t.start();
		}
		
		for (Thread t : applicationsThread) {
			t.join();
		}
		
		long total0 = 0;
		long total1 = 0;
		long total2 = 0;
		long total3 = 0;
		long total4 = 0;
		for (int i = 0; i < nbApp; i++) {
			total0 += res.get((nbApp * 0) + i);
			total1 += res.get((nbApp * 1) + i);
			total2 += res.get((nbApp * 2) + i);
			total3 += res.get((nbApp * 3) + i);
			total4 += res.get((nbApp * 4) + i);
		}
		total0 /= nbApp;
		total1 /= nbApp;
		total2 /= nbApp;
		total3 /= nbApp;
		total4 /= nbApp;
		
		System.out.println(" !!!!!!!!!!moyenne de temps profil 0 = " + total0);
		System.out.println(" !!!!!!!!!!moyenne de temps profil 1 = " + total1);
		System.out.println(" !!!!!!!!!!moyenne de temps profil 2 = " + total2);
		System.out.println(" !!!!!!!!!!moyenne de temps profil 3 = " + total3);
		System.out.println(" !!!!!!!!!!moyenne de temps profil 4 = " + total4);
		
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
