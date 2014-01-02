package test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import monitor.Monitor;

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

public class TestMonitor {
	
	static String storeName = "kvstore";
	static String hostName = "ari-31-201-01";
	static int hostPort = 31500;
	static List<KVDB> kvdbs = new ArrayList<KVDB>();
	static List<Monitor> monitors = new ArrayList<Monitor>();
	
	@BeforeClass
	public static void onlyOnce() {
		//create DBs
	    int temp = hostPort;
	    for (int i = 0; i < 2; i++) {
			kvdbs.add(new KVDB(i * 5, storeName, hostName, new Integer(temp).toString(), null));
			temp+=2;
	    }
	    
	    monitors.add(new Monitor(kvdbs, 0));
	    //monitors.add(new Monitor(kvdbs.subList(2, 3), 5));
	}
	
	@AfterClass
	public static void after() {
		for (KVDB db : kvdbs) {
			db.closeDB();
	    }
	}
	
	@Test
	public void testInit() {
		//System.out.println(monitors);
	}
	
	@Test
	public void testTransationStandard() {
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
