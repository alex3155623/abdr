package db;

public class KVDBLoadBalancer implements Runnable {
	private KVDB kvdb;
	private KVDB leftKVDB;
	private KVDB rightKVDB;
	
	
	
	public KVDBLoadBalancer (KVDB kvdb) {
		this.kvdb = kvdb;
		
		int kvdbsCount = kvdb.getKvdbs().size();
		int id = kvdb.getId() + kvdbsCount;
		//leftKVDB = kvdb.getKvdbs().get((id - 1) % kvdbsCount);
		//rightKVDB = kvdb.getKvdbs().get((id + 1) % kvdbsCount);
	}
	
	@Override
	public void run() {
		
		
		/*
		while (true) {
			
		}*/
	}

}
