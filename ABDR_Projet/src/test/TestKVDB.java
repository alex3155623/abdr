package test;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import db.KVDBInterface;

public class TestKVDB {
	static String storeName = "kvstore";
	static String hostName = "ari-31-201-01";
	static int hostPort = 31500;
	static Map<Integer, KVDB> kvdbs = new HashMap<Integer, KVDB>();
	static Map<Integer, Monitor> monitors = new HashMap<Integer, Monitor>();
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
	    
	    Set<Integer> keys = kvdbs.keySet();
	    for (Integer kvdbIndex : keys) {
	    	kvdbs.get(kvdbIndex).startDB();
	    }
	    
	    //init monitors
	    for (int i = 0; i < nbProfilePerKVDB; i++) {
	    	monitors.put(i, new Monitor(tempList, 0));
	    }
	}
	
	@AfterClass
	public static void after() {
		Set<Integer> keys = kvdbs.keySet();
		for (Integer dbIndex : keys) {
			kvdbs.get(dbIndex).closeDB();
	    }
	}
	
	@Test
	public void testAddDelete() {
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
		List<OperationResult> results = kvdbs.get(0).executeOperations(operations);
		
		assertEquals(1, results.size());
		assertTrue(results.get(0).isSuccess());
		
		//il faut que cet element existe dans la db, on check
		data = new Data();
		data.setCategory(category);
		data.setId(id);
		operations = new ArrayList<Operation>();
		operations.add(new ReadOperation(data));
		
		results = kvdbs.get(0).executeOperations(operations);
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
		
		results = kvdbs.get(0).executeOperations(operations);
		assertEquals(1, results.size());
		assertTrue(results.get(0).isSuccess());
		
		//il faut que cet element ai disparu de la db, on check
		data = new Data();
		data.setCategory(category);
		data.setId(id);
		operations = new ArrayList<Operation>();
		operations.add(new ReadOperation(data));
		results = kvdbs.get(0).executeOperations(operations);
		assertEquals(1, results.size());
		assertTrue(! results.get(0).isSuccess());
	}
	
	@Test
	public void testTransactionMono() {
		int category = 4;
		int id = 42;
		
		List<Data> datas = new ArrayList<Data>();
		Data data;
		for (int i = 0; i < 3; i++) {
			data = new Data();
			data.setCategory(category);
			data.setId(id + i);
			data.getListNumber().add(700 + i);
			data.getListNumber().add(701 + i);
			data.getListNumber().add(702 + i);
			data.getListNumber().add(703 + i);
			data.getListNumber().add(704 + i);
			
			data.getListString().add("s" + i);
			data.getListString().add("s" + i);
			data.getListString().add("s" + i);
			data.getListString().add("s" + i);
			data.getListString().add("s" + i);
			
			datas.add(data);
		}
		
		//creation d'une transaction pour ajouter 3 elements
		List<Operation> operations = new ArrayList<Operation>();
		operations.add(new WriteOperation(datas.get(0)));
		operations.add(new WriteOperation(datas.get(1)));
		operations.add(new WriteOperation(datas.get(2)));
		
		//on ajoute ces elements
		List<OperationResult> results = kvdbs.get(0).executeOperations(operations);
		
		assertEquals(3, results.size());
		
		for (int i = 0; i < results.size(); i++)
			assertTrue(results.get(i).isSuccess());
		
		//il faut que ces elements existent dans la db, on check
		operations = new ArrayList<Operation>();
		data = new Data();
		data.setCategory(category);
		data.setId(id + 1);
		operations.add(new ReadOperation(data));
		
		results = kvdbs.get(0).executeOperations(operations);
		assertEquals(1, results.size());
		assertTrue(results.get(0).isSuccess());
		assertEquals(5, results.get(0).getData().getListNumber().size());
		assertEquals(5, results.get(0).getData().getListString().size());
		assertEquals(700 + 1,  0 + results.get(0).getData().getListNumber().get(0));
		assertEquals(701 + 1,  0 + results.get(0).getData().getListNumber().get(1));
		assertEquals(702 + 1,  0 + results.get(0).getData().getListNumber().get(2));
		assertEquals(703 + 1,  0 + results.get(0).getData().getListNumber().get(3));
		assertEquals(704 + 1,  0 + results.get(0).getData().getListNumber().get(4));
		assertEquals("s" + 1,  results.get(0).getData().getListString().get(0));
		assertEquals("s" + 1,  results.get(0).getData().getListString().get(1));
		assertEquals("s" + 1,  results.get(0).getData().getListString().get(2));
		assertEquals("s" + 1,  results.get(0).getData().getListString().get(3));
		assertEquals("s" + 1,  results.get(0).getData().getListString().get(4));
		
		operations = new ArrayList<Operation>();
		data = new Data();
		data.setCategory(category);
		data.setId(id + 2);
		operations.add(new ReadOperation(data));
		
		results = kvdbs.get(0).executeOperations(operations);
		assertEquals(1, results.size());
		assertTrue(results.get(0).isSuccess());
		assertEquals(5, results.get(0).getData().getListNumber().size());
		assertEquals(5, results.get(0).getData().getListString().size());
		assertEquals(700 + 2,  0 + results.get(0).getData().getListNumber().get(0));
		assertEquals(701 + 2,  0 + results.get(0).getData().getListNumber().get(1));
		assertEquals(702 + 2,  0 + results.get(0).getData().getListNumber().get(2));
		assertEquals(703 + 2,  0 + results.get(0).getData().getListNumber().get(3));
		assertEquals(704 + 2,  0 + results.get(0).getData().getListNumber().get(4));
		assertEquals("s" + 2,  results.get(0).getData().getListString().get(0));
		assertEquals("s" + 2,  results.get(0).getData().getListString().get(1));
		assertEquals("s" + 2,  results.get(0).getData().getListString().get(2));
		assertEquals("s" + 2,  results.get(0).getData().getListString().get(3));
		assertEquals("s" + 2,  results.get(0).getData().getListString().get(4));

		
		//tentative de suppression
		datas.clear();
		for (int i = 0; i < 3; i++) {
			data = new Data();
			data.setCategory(category);
			data.setId(id + i);
			
			datas.add(data);
		}
		
		operations.clear();
		operations.add(new DeleteOperation(datas.get(0)));
		operations.add(new DeleteOperation(datas.get(1)));
		operations.add(new DeleteOperation(datas.get(2)));
		results = kvdbs.get(0).executeOperations(operations);
		assertEquals(3, results.size());
		assertTrue(results.get(0).isSuccess());
		
		
		//il faut que cet element ai disparu de la db, on check
		operations = new ArrayList<Operation>();
		data = new Data();
		data.setCategory(category);
		data.setId(id + 1);
		operations.add(new ReadOperation(data));
		results = kvdbs.get(0).executeOperations(operations);
		
		assertEquals(1, results.size());
		assertTrue(! results.get(0).isSuccess());
		
		operations = new ArrayList<Operation>();
		data = new Data();
		data.setCategory(category);
		data.setId(id + 2);
		operations.add(new ReadOperation(data));
		results = kvdbs.get(0).executeOperations(operations);
		
		assertEquals(1, results.size());
		assertTrue(! results.get(0).isSuccess());
	}
	
	@Test
	public void testTransfusion() {
		/*List<Integer> transfusedProfiles = new ArrayList<Integer>();
		transfusedProfiles.add(1);
		transfusedProfiles.add(3);
		
		System.out.println(" ------------------------DB0 avant");
		kvdbs.get(0).printDB();
		System.out.println(" ------------------------DB1 avant");
		kvdbs.get(5).printDB();
		
		kvdbs.get(0).transfuseData(transfusedProfiles, kvdbs.get(5));
		
		System.out.println(" ------------------------DB0 apres");
		kvdbs.get(0).printDB();
		System.out.println(" ------------------------DB1 apres");
		kvdbs.get(5).printDB();*/
	}
	
	
	@Test
	public void testMigration() {
		/*List<Integer> transfusedProfiles = new ArrayList<Integer>();
		transfusedProfiles.add(1);
		transfusedProfiles.add(3);
		
		System.out.println(" ------------------------DB0 avant");
		kvdbs.get(0).printDB();
		System.out.println(" ------------------------DB1 avant");
		kvdbs.get(5).printDB();
		
		kvdbs.get(5).migrate(transfusedProfiles);
		
		System.out.println(" ------------------------DB0 apres");
		kvdbs.get(0).printDB();
		System.out.println(" ------------------------DB1 apres");
		kvdbs.get(5).printDB();*/
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
