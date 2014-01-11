package test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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

import transaction.Data;
import transaction.DeleteOperation;
import transaction.Operation;
import transaction.OperationResult;
import transaction.ReadOperation;
import transaction.WriteOperation;
import db.KVDB;
import db.KVDBInterface;

public class TestMonitor {
	
	static String storeName = "kvstore";
	static String hostName = "ari-31-201-07";
	static int hostPort = 31500;
	static int rmiPort = 55000;
	static Map<Integer, KVDBInterface> kvdbs = new HashMap<Integer, KVDBInterface>();
	static Map<Integer, MonitorInterface> monitors = new HashMap<Integer, MonitorInterface>();
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
	    	int tempId = (i * nbProfilePerKVDB);

			try {
				kvdbs.get(tempId).setLeftKVDB(kvdbs.get((tempId - nbProfilePerKVDB + (nbKVDB * nbProfilePerKVDB)) % (nbKVDB * nbProfilePerKVDB)));
				kvdbs.get(tempId).setRightKVDB(kvdbs.get((tempId + nbProfilePerKVDB + (nbKVDB * nbProfilePerKVDB)) % (nbKVDB * nbProfilePerKVDB)));
				kvdbs.get(tempId).setMonitors(monitors);
				kvdbs.get(tempId).setSelf(kvdbs.get(tempId));
			} catch (RemoteException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    }
	    
	    Set<Integer> keys = kvdbs.keySet();
	    for (Integer kvdbIndex : keys) {
	    	//kvdbs.get(kvdbIndex).startDB();
	    }
	}
	
	@AfterClass
	public static void after() throws RemoteException {
		Set<Integer> keys = kvdbs.keySet();
		for (Integer dbIndex : keys) {
			kvdbs.get(dbIndex).closeDB();
	    }
	}
	
	@Test
	public void testInit() {
		//System.out.println(monitors);
	}
	
	@Test
	public void testTransationStandard() throws RemoteException {
		int category = 4;
		int id = 42;
		Data data = new Data();
		data.setCategory(category);
		data.setId(id);
		data.getListNumber().add(700);
		data.getListNumber().add(701);
		data.getListNumber().add(702);
		data.getListNumber().add(703);
		data.getListNumber().add(704);
		
		data.getListString().add("s1");
		data.getListString().add("s2");
		data.getListString().add("s3");
		data.getListString().add("s4");
		data.getListString().add("s5");
		
		//creation d'une transaction pour ajouter 1 element
		List<Operation> operations = new ArrayList<Operation>();
		operations.add(new WriteOperation(data));
		
		//on ajoute cet element
		List<OperationResult> results = monitors.get(0).executeOperations(operations);
		
		assertEquals(1, results.size());
		assertTrue(results.get(0).isSuccess());
		
		//il faut que cet element existe dans la db, on check
		data = new Data();
		data.setCategory(category);
		data.setId(id);
		operations = new ArrayList<Operation>();
		operations.add(new ReadOperation(data));
		
		results = monitors.get(0).executeOperations(operations);
		assertEquals(1, results.size());
		assertTrue(results.get(0).isSuccess());
		assertEquals(5, results.get(0).getData().getListNumber().size());
		assertEquals(5, results.get(0).getData().getListString().size());
		assertEquals(700,  0 + results.get(0).getData().getListNumber().get(0));
		assertEquals(701,  0 + results.get(0).getData().getListNumber().get(1));
		assertEquals(702,  0 + results.get(0).getData().getListNumber().get(2));
		assertEquals(703,  0 + results.get(0).getData().getListNumber().get(3));
		assertEquals(704,  0 + results.get(0).getData().getListNumber().get(4));
		assertEquals("s1",  results.get(0).getData().getListString().get(0));
		assertEquals("s2",  results.get(0).getData().getListString().get(1));
		assertEquals("s3",  results.get(0).getData().getListString().get(2));
		assertEquals("s4",  results.get(0).getData().getListString().get(3));
		assertEquals("s5",  results.get(0).getData().getListString().get(4));

		//tentative de suppression
		data = new Data();
		data.setCategory(category);
		data.setId(id);
		operations = new ArrayList<Operation>();
		operations.add(new DeleteOperation(data));
		
		results = monitors.get(0).executeOperations(operations);
		assertEquals(1, results.size());
		assertTrue(results.get(0).isSuccess());
		
		//il faut que cet element ai disparu de la db, on check
		data = new Data();
		data.setCategory(category);
		data.setId(id);
		operations = new ArrayList<Operation>();
		operations.add(new ReadOperation(data));
		results = monitors.get(0).executeOperations(operations);
		assertEquals(1, results.size());
		assertTrue(! results.get(0).isSuccess());
	}
	
	

}
