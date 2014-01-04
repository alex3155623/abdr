package monitor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import transaction.Operation;
import transaction.OperationResult;
import db.KVDB;

/**
 * aiguilleur + trie la transaction (donne la transaction à la machine ayant la plus petite ressource)
 * donne l'ordre de migration (kvdb migre les table peu utilisées vers une autre db)

	- possède catalogue
 * 
 * @author 2600705
 *
 */
public class Monitor implements MonitorInterface{
	private List<KVDB> servers = new ArrayList<KVDB>();
	private Map<Integer, Monitor> monitorMapping = new ConcurrentHashMap<Integer, Monitor>();
	private Map<Integer, KVDB> serverMapping = new ConcurrentHashMap<Integer, KVDB>();
	
	//mutex to access profiles. (reader = transactions thread, writer = monitor).
	private Map<Integer, ReadWriteLock> profileMutexes = new ConcurrentHashMap<Integer, ReadWriteLock>();
	private int nbProfile = 10;
	private int profileOffset;
	private int nbMonitors = 1;
	
	
	public Monitor(List<KVDB> kvdbs) {
		servers = kvdbs;
		initAll();
	}
	
	public Monitor(List<KVDB> kvdbs, int profileOffset) {
		servers = kvdbs;
		this.profileOffset = profileOffset;
		initAll();
	}
	
	private void initMutexes() {
		for (int i = profileOffset; i < profileOffset + nbProfile; i++) {
			profileMutexes.put(i, new ReentrantReadWriteLock(true));
		}
	}
	
	private void initMapping() {
		int tempOffset = profileOffset;
		
		for (int i = 0; i < nbMonitors; i++) {
			//monitorMapping.put
		}
		
		for (KVDB currentServer : servers) {
			for (int i = 0; (i < 5) && (tempOffset < (profileOffset + nbProfile)); tempOffset++, i++) {
				serverMapping.put(tempOffset, currentServer);
			}
		}
	}
	
	private void initAll() {
		initMutexes();
		initMapping();
	}
	

	/**
	 *  Execute a list of operation. Entry point of applications
	 */
	@Override
	public List<OperationResult> executeOperations(List<Operation> operations) {
		//sort the transaction
		operations = sortTransaction(operations);
		
		//get the list of needed local profiles for the transaction
		List<Integer> usedLocalProfiles = findProfile(operations);
		
		//for each profiles in the transaction, read lock
		synchronized (this) {
			for (Integer profile : usedLocalProfiles) {
				profileMutexes.get(profile).readLock().lock();
			}
		}

		// search the good kvstore and execute operation in kvdb
		KVDB targetServer = findKVDB(operations);
		
		//execute the transaction on it
		List<OperationResult> results = targetServer.executeOperations(operations);
		
		synchronized (this) {
			for (Integer profile : usedLocalProfiles) {
				profileMutexes.get(profile).readLock().unlock();
			}
		}
		
		return results;
	}

	
	private List<Operation> sortTransaction(List<Operation> operations) {
		Collections.sort(operations, new Comparator<Operation>() {
			  public int compare(Operation op1, Operation op2) {
			     int data1 = op1.getData().getCategory();
			     int data2 = op2.getData().getCategory();
			     if (data2 < data1) return 1;
			     else if(data2 == data1) return 0;
			     else return -1;
			  }
		});
		
		return operations;
	}

	
	private List<Integer> findProfile(List<Operation> operations) {
		int currentProfile;
		ArrayList<Integer> list = new ArrayList<Integer>();
		for(Operation op : operations){
			currentProfile = op.getData().getCategory();
			if ((currentProfile >= profileOffset) && (currentProfile < (profileOffset + nbProfile) && (! list.contains(currentProfile))))
				list.add(currentProfile);
		}
		
		return list;
	}
	
	/**
	 * Find the KVDB which will execute the transaction (it's the server having the 
	 * most low category)
	 * @param operations
	 * @return
	 */
	private KVDB findKVDB(List<Operation> operations) {
		//TODO traiter le cas où la catégorie m'appartient pas 
		return serverMapping.get(operations.get(0).getData().getCategory());
	}

	@Override
	public KVDB notifyMigration(KVDB newSource, int profile) {
		KVDB result;
		synchronized(this) {
			profileMutexes.get(profile).writeLock().lock();
		}
		result = serverMapping.get(profile);
		
		return result;
	}
	
	@Override
	public void notifyEndMigration(KVDB newSource, int profile){
		serverMapping.put(profile, newSource);
		synchronized(this) {
			profileMutexes.get(profile).writeLock().unlock();
		}
	}

	
	@Override
	public String toString() {
		return "Monitor [serverMapping=" + serverMapping + ", nbProfile="
				+ nbProfile + ", profileOffset=" + profileOffset + "]";
	}
	
	
}
