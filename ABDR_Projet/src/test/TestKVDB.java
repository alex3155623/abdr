package test;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import transaction.Data;
import db.KVDB;

public class TestKVDB {
	static String storeName = "kvstore";
	static String hostName = "ari-31-201-02";
	static int hostPort = 5002;
	static List<KVDB> kvdbs = new ArrayList<KVDB>();
	
	@BeforeClass
	public static void onlyOnce() {
		//create DBs
	    int temp = hostPort;
	    for (int i = 0; i < 2; i++) {
			kvdbs.add(new KVDB(i, storeName, hostName, new Integer(temp).toString()));
			temp += 2;
	    }
	}
	
	@Test
	public void testPrivateAddDelete() {
		int category = 5;
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
		
		kvdbs.get(0).addData(data);
		
		Data res = kvdbs.get(0).getData(id, category);
		assertNotNull(res);
		assertEquals(700, res.getListNumber().get(0).intValue());
		assertEquals(701, res.getListNumber().get(1).intValue());
		assertEquals(702, res.getListNumber().get(2).intValue());
		assertEquals(703, res.getListNumber().get(3).intValue());
		assertEquals(704, res.getListNumber().get(4).intValue());
		
		assertEquals("s1", res.getListString().get(0));
		assertEquals("s2", res.getListString().get(1));
		assertEquals("s3", res.getListString().get(2));
		assertEquals("s4", res.getListString().get(3));
		assertEquals("s5", res.getListString().get(4));
		
		kvdbs.get(0).removeData(data);
		
		res = kvdbs.get(0).getData(id, category);
		assertNull(res);
	}
	
	

}
