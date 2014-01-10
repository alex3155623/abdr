package monitor;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import myReadWriteLock.MyReadWriteLock;
import transaction.Operation;
import transaction.OperationResult;
import db.KVDBInterface;

public class MonitorImplementation extends UnicastRemoteObject implements MonitorInterface {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Map<Integer, Monitor> monitorMapping = new ConcurrentHashMap<Integer, Monitor>();
	private Map<Integer, KVDBInterface> serverMapping = new ConcurrentHashMap<Integer, KVDBInterface>();
	
	//mutex to access profiles. (reader = transactions thread, writer = monitor).
	private Map<Integer, MyReadWriteLock> profileMutexes = new ConcurrentHashMap<Integer, MyReadWriteLock>();
	private int nbProfile = 10;
	private int profileOffset;
	private int nbMonitors = 1;
	
	
	public MonitorImplementation(Map<Integer, KVDBInterface> kvdbs, int profileOffset) throws RemoteException {
		this.serverMapping = kvdbs;
		this.profileOffset = profileOffset;
		
		initAll();
	}
	
	private void initMutexes() {
		for (int i = profileOffset; i < profileOffset + nbProfile; i++) {
			//profileMutexes.put(i, new ReentrantReadWriteLock(true));
			profileMutexes.put(i, new MyReadWriteLock());
		}
	}
	
	private void initAll() {
		initMutexes();
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
	private KVDBInterface findKVDB(List<Operation> operations) {
		//TODO traiter le cas où la catégorie m'appartient pas 
		return serverMapping.get(operations.get(0).getData().getCategory());
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
		System.out.println("***********************local profiles of the monitor : " + usedLocalProfiles + " for operations" + operations);
		
		//for each profiles in the transaction, read lock
		for (Integer profile : usedLocalProfiles) {
			profileMutexes.get(profile).lockRead();
			System.out.println("readlock : " + profile + " of application " + operations.get(0).getData().sourceId + ", offest = " + profileOffset);
		}

		// search the good kvstore and execute operation in kvdb
		KVDBInterface targetServer = findKVDB(operations);
		
		//execute the transaction on it
		List<OperationResult> results = null;
		try {
			results = targetServer.executeOperations(operations);
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		for (Integer profile : usedLocalProfiles) {
			System.out.println("readunlock : " + profile + " of application " + operations.get(0).getData().sourceId);
			profileMutexes.get(profile).unlockRead();
		}
		
		return results;
	}


	@Override
	public KVDBInterface notifyLoadBalanceMigration(KVDBInterface newSource, int profile) {
		KVDBInterface result;
		System.out.println("trying to writelock " + profile);
		profileMutexes.get(profile).lockWrite();
		System.out.println("writelock " + profile + " successful");

		result = serverMapping.get(profile);
		
		return result;
	}
	
	@Override
	public void notifyEndLoadBalanceMigration(KVDBInterface newSource, int profile){
		serverMapping.put(profile, newSource);
		System.out.println("trying to writeunlock " + profile);
		profileMutexes.get(profile).unlockWrite();
		System.out.println("writeunlock " + profile + " successful");
	}
	
	
	@Override
	public String toString() {
		return "Monitor [serverMapping=" + serverMapping + ", nbProfile="
				+ nbProfile + ", profileOffset=" + profileOffset + "]";
	}

	@Override
	public KVDBInterface notifyStandardMigration(KVDBInterface newSource, int profile) throws RemoteException {
		KVDBInterface result;
		System.out.println("trying to get standard migration lock de " + profile + ", offest = " + profileOffset);
		profileMutexes.get(profile).unlockRead();
		profileMutexes.get(profile).lockWrite();
		System.out.println("standard migration lock OK de " + ", offest = " + profileOffset);
		
		result = serverMapping.get(profile);
		
		return result;
	}

	@Override
	public void notifyEndStandardMigration(KVDBInterface newSource, int profile) throws RemoteException {
		System.out.println("trying to get standard migration unlock de " + profile + ", offest = " + profileOffset);
		
		profileMutexes.get(profile).lockReadAfterWrite();
		profileMutexes.get(profile).unlockWrite();
		System.out.println("standard migration unlock OK de " + ", offest = " + profileOffset);
	}
}
