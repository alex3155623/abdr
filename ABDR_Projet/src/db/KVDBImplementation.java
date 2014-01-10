package db;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import monitor.MonitorInterface;
import oracle.kv.DurabilityException;
import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.KeyRange;
import oracle.kv.OperationExecutionException;
import oracle.kv.OperationFactory;
import oracle.kv.Value;
import oracle.kv.ValueVersion;
import transaction.Data;
import transaction.DeleteOperation;
import transaction.Operation;
import transaction.OperationResult;
import transaction.ReadOperation;
import transaction.WriteOperation;

public class KVDBImplementation extends UnicastRemoteObject implements KVDBInterface {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private KVStore store;
	private int id;
    private String storeName = "kvstore";
    private String hostName = "localhost";
    private String hostPort = "5000";
    
    private final int nbInt = 5;
    private final int nbString = 5;
    private final int nbObjects = 2;
    private final int nbProfile = 5;

    private Map<String, KVDBInterface> neighbourKvdbs = new HashMap<String, KVDBInterface>();
    private Map<Integer, MonitorInterface> monitorMapping = new ConcurrentHashMap<Integer, MonitorInterface>();
    private Map<Integer, Integer> localProfiles = new ConcurrentHashMap<Integer, Integer>();
    private Map<Integer, ReadWriteLock> profileMutexes = new ConcurrentHashMap<Integer, ReadWriteLock>();

    private Thread loadBalancer;
    private boolean runLoadBalancer = true;
    private Map<Integer, TokenInterface> tokens = new ConcurrentHashMap<Integer, TokenInterface>();
    
	
	public KVDBImplementation(int id, String storeName, String hostName, String hostPort) throws RemoteException {
		this.id = id;
		this.storeName = storeName;
		this.hostName = hostName;
		this.hostPort = hostPort;
		initAll();
	}
	
	
	@Override
	public int getKVDBId() {
		return id;
	}
	
	@Override
	public void setLeftKVDB(KVDBInterface kvdbLeft) {
		this.neighbourKvdbs.put("left", kvdbLeft);
	}
	
	@Override
	public void setRightKVDB(KVDBInterface kvdbRight) {
		this.neighbourKvdbs.put("right", kvdbRight);
	}
	
	@Override
	public void setMonitors(Map<Integer, MonitorInterface> monitors) {
		this.monitorMapping = monitors;
	}

	public Map<String, KVDBInterface> getNeighbourKvdbs() {
		return neighbourKvdbs;
	}

	
	private void initBase() {
        Key key;

        //instanciation de la base de donnée
		try {
		      store = KVStoreFactory.getStore(new KVStoreConfig(storeName, hostName + ":" + hostPort));
		} catch (Exception e) {
		    e.printStackTrace();
		}
		
		for (int i = 0; i < 100; i++) {
			store.multiDelete(Key.createKey("" + i), null, null);
		}
        
        //foreach profile
		for (int i = id; i < (id + nbProfile); i++) {
			localProfiles.put(i, i);
			//profileMutexes.put(i, new ReentrantReadWriteLock(true));

			//foreach object
			for (int j = 0; j < nbObjects; j++) {
				//foreach attribute
				for (int k = 0; k < nbString + nbInt; k++) {
					List<String> att = new ArrayList<String>();
					att.add(new Integer(j).toString());
					att.add(new Integer(k).toString());
					key = Key.createKey("" + i,  att);
					store.put(key, Value.createValue(new Integer(0).toString().getBytes()));
				}
			}
		}
    }
	
	private void initAll() {
		initBase();
	}
	
	
	private List<Integer> getUnknownProfiles(List<Operation> operations) {
		List<Integer> unknownProfiles = new ArrayList<Integer>();
		synchronized (localProfiles) {
			for (Operation operation : operations) {
				int currentProfile = operation.getData().getCategory();
				if ((! localProfiles.containsKey(currentProfile)) && (! unknownProfiles.contains(currentProfile)))
					unknownProfiles.add(currentProfile);
			}
		}
		
		return unknownProfiles;
	}
	
	private List<Integer> getTransactionProfiles(List<Operation> operations) {
		Map<Integer, Integer> transactionProfiles = new HashMap<Integer, Integer>();
		
		for (Operation operation : operations) {
			int currentProfile = operation.getData().getCategory();
			if (! transactionProfiles.containsKey(currentProfile))
				transactionProfiles.put(currentProfile, currentProfile);
		}
		
		return new ArrayList<Integer>(transactionProfiles.values());
	}
	
	
	private List<Integer> implodeProfiles(List<Integer> profiles) {
		List<String> prim = new ArrayList<String>();
		for (int c : profiles) {
    		prim.add(c + "");
		}

		for (int profile : profiles) {
			//get every values for this profile
			SortedMap<Key,ValueVersion> profileObjects = store.multiGet(Key.createKey(profile + ""), null, null);
			
			//for every values, insert them to their new profile and delete it from it's old one
			for (Entry<Key, ValueVersion> profileObject : profileObjects.entrySet()) {
				Key oldKey = profileObject.getKey();
				Value value = profileObject.getValue().getValue();
				List<String> newMinorKey = oldKey.getFullPath();
				Key key = Key.createKey(prim, newMinorKey);
				
				store.put(key, value);
				store.delete(oldKey);
			}
		}
		
		return profiles;
	}
	
	private void explodeProfile(List<Integer> fusedProfilesMasterKey) {
		List<String> prim = new ArrayList<String>();
		for (int partialFused : fusedProfilesMasterKey) {
			prim.add(partialFused + "");
		}
		
		SortedMap<Key,ValueVersion> profileObjects = store.multiGet(Key.createKey(prim), null, null);
		//for every values, insert them to their old profile and delete it from it's temporary one
		for (Entry<Key, ValueVersion> profileObject : profileObjects.entrySet()) {
			Key oldKey = profileObject.getKey();
			Value value = profileObject.getValue().getValue();
			List<String> newMinorKey = oldKey.getMinorPath().subList(1, oldKey.getMinorPath().size());
			List<String> newMajorKey = oldKey.getMinorPath().subList(0, 1);
			Key key = Key.createKey(newMajorKey, newMinorKey);
			
			store.put(key, value);
			store.delete(oldKey);
		}
	}
	
	
	/**
	 * entry point of monitors transactions
	 */
	@Override
	public List<OperationResult> executeOperations(List<Operation> operations) {
		List<OperationResult> result = new ArrayList<OperationResult>();
		System.out.println("KVDB " + id + " solicité pour " + getTransactionProfiles(operations));
		
		//get on a single data
		if ((operations.size() == 1) && (operations.get(0) instanceof ReadOperation)) {
			//profileMutexes.get(operations.get(0).getData().getCategory()).readLock().lock();
			result = getData(operations.get(0).getData());
			//profileMutexes.get(operations.get(0).getData().getCategory()).readLock().unlock();
		}
		//single key transaction (we should have this key so we don't check)
		else if (getTransactionProfiles(operations).size() == 1) {
			////System.out.println(id + " single key transaction with key " + getTransactionProfiles(operations).get(0) + ", list of op = " + operations);
			//profileMutexes.get(operations.get(0).getData().getCategory()).readLock().lock();
			////System.out.println(id + " target locked!!!!!");
			result = internalExecute(convertOperations(operations));
			////System.out.println(id + " exe OK");
			//profileMutexes.get(operations.get(0).getData().getCategory()).readLock().unlock();
		}
		else {
			//multiple key transaction
			List<Integer> unknownProfiles = getUnknownProfiles(operations);
			List<Integer> transactionProfiles = getTransactionProfiles(operations);
			
			//worst case execution, we need to fetch before executing the multiple key transaction
			if (unknownProfiles.size() != 0) {
				//System.out.println("KVDB " + id + " migre " + unknownProfiles + " pour transaction");
				migrate(unknownProfiles, false);
			}

			//System.out.println("KVDB " + id + " lock transaction " + getTransactionProfiles(operations));
			for (int profile : transactionProfiles) {
				try {
					//profileMutexes.get(profile).writeLock().lock();
				} catch (Exception e) {
					//System.out.println(id + " Exception trying to lock " + profile);
					e.printStackTrace();
				}
			}
			//System.out.println("KVDB " + id + " fait transaction " + getTransactionProfiles(operations));
			List<Integer> tempProfile = implodeProfiles(transactionProfiles);

			//execute multikey transaction
			result = internalExecute(convertOperations(operations, tempProfile));
			
			explodeProfile(tempProfile);
			//System.out.println("KVDB " + id + " a fini transaction " + getTransactionProfiles(operations));
			
			for (int profile : transactionProfiles) {
				try {
					//profileMutexes.get(profile).writeLock().unlock();
				} catch (Exception e) {
					//System.out.println(id + " Exception trying to unlock " + profile + " for transaction " + transactionProfiles);
					e.printStackTrace();
				}
			}
			////System.out.println("KVDB " + id + " a totalement fini transaction " + getTransactionProfiles(operations));
		}
		
		return result;
	}
	
	private List<oracle.kv.Operation> convertOperations (List<Operation> operations, List<Integer> newPrimaryKey) {
		List<oracle.kv.Operation> operationList = new ArrayList<oracle.kv.Operation>();

		for (Operation operation : operations) {
			if (operation instanceof WriteOperation) {
				operationList.addAll(getAddDataTransaction(operation.getData(), newPrimaryKey));
			}
			else {
				operationList.addAll(getRemoveDataTransaction(operation.getData(), newPrimaryKey));
			}
		}
		
		return operationList;
	}
	
	
	private List<oracle.kv.Operation> convertOperations (List<Operation> operations) {
		List<oracle.kv.Operation> operationList = new ArrayList<oracle.kv.Operation>();

		for (Operation operation : operations) {
			if (operation instanceof WriteOperation) {
				operationList.addAll(getAddDataTransaction(operation.getData()));
			}
			else {
				operationList.addAll(getRemoveDataTransaction(operation.getData()));
			}
		}
		
		return operationList;
	}
	
	
	private List<OperationResult> internalExecute(List<oracle.kv.Operation> operations) {
		boolean hasExecuted = false;
		List<OperationResult> result = new ArrayList<OperationResult>();
		do {
		    try {
		    	////System.out.println(id + " tentative exe");
				List<oracle.kv.OperationResult> res = store.execute(operations);
				result.addAll(KVResult2OperationResult(res));
				hasExecuted = true;
				
			} catch (DurabilityException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (OperationExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (FaultException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} while (! hasExecuted);
		
		return result;
	}
	
	/**
	 * Get data object associated with an id and category
	 * @param data
	 * @return
	 */
	private List<OperationResult> getData (Data data) {
		int dataId = data.getId();
		int category = data.getCategory();
		List <OperationResult> operationResult = new ArrayList<OperationResult>();
		
		SortedMap<Key,ValueVersion> profileObjects = store.multiGet(Key.createKey("" + category), new KeyRange (new Integer(dataId).toString()), null);

		if (profileObjects.size() != (nbString + nbInt)) {
			operationResult.add(new OperationResult(false, null));
			return operationResult;
		}
		
		int i = 0;
		for (Entry<Key, ValueVersion> profileObject : profileObjects.entrySet()) {
			String value = new String (profileObject.getValue().getValue().getValue());
			if (i < nbInt)
				data.getListNumber().add(Integer.valueOf(value));
    		else
    			data.getListString().add(value);
    		i++;
		}
		
		operationResult.add(new OperationResult(true, data));
		
		return operationResult;
	}
	
	
	private List<OperationResult> KVResult2OperationResult (List<oracle.kv.OperationResult> kvResult) {
		List <OperationResult> operationResult = new ArrayList<OperationResult>();
		for (int i = 0; i < kvResult.size(); i += 10) {
			operationResult.add(new OperationResult(kvResult.get(i).getSuccess(), null));
		}

		return operationResult;
	}
	
	/**
	 * get the transaction matching the operation. if new primary key is set, shift keys
	 * @param data
	 * @param newPrimaryKey
	 * @return
	 */
	private List<oracle.kv.Operation> getAddDataTransaction (Data data) {
		int dataId = data.getId();
		int category = data.getCategory();
		Key key;
		OperationFactory operationFactory = store.getOperationFactory();
		List<oracle.kv.Operation> operationList = new ArrayList<oracle.kv.Operation>();
		
		for (int i = 0; i < nbInt + nbString; i++) {
			List<String> att = new ArrayList<String>();
    		att.add(new Integer(dataId).toString());
    		att.add(new Integer(i).toString());
    		key = Key.createKey("" + category, att);
    		
    		if (i < nbInt)
    			operationList.add(operationFactory.createPut(key, Value.createValue(data.getListNumber().get(i).toString().getBytes())));
    		else
    			operationList.add(operationFactory.createPut(key, Value.createValue(data.getListString().get(i - nbInt).toString().getBytes())));
		}
		
		return operationList;
	}
	
	/**
	 * get the transaction matching the operation. if new primary key is set, shift keys
	 * @param data
	 * @param newPrimaryKey
	 * @return
	 */
	private List<oracle.kv.Operation> getRemoveDataTransaction (Data data) {
		int dataId = data.getId();
		int category = data.getCategory();
		Key key;
		OperationFactory operationFactory = store.getOperationFactory();
		List<oracle.kv.Operation> operationList = new ArrayList<oracle.kv.Operation>();
		
		for (int i = 0; i < nbInt + nbString; i++) {
			List<String> att = new ArrayList<String>();
    		att.add(new Integer(dataId).toString());
    		att.add(new Integer(i).toString());
    		key = Key.createKey("" + category, att);
    		operationList.add(operationFactory.createDelete(key));
		}
		
		return operationList;
	}
	
	/**
	 * get the transaction matching the operation. if new primary key is set, shift keys
	 * @param data
	 * @param newPrimaryKey
	 * @return
	 */
	private List<oracle.kv.Operation> getAddDataTransaction (Data data, List<Integer> newPrimaryKey) {
		int dataId = data.getId();
		int category = data.getCategory();
		Key key;
		OperationFactory operationFactory = store.getOperationFactory();
		List<oracle.kv.Operation> operationList = new ArrayList<oracle.kv.Operation>();
		
		for (int i = 0; i < nbInt + nbString; i++) {
			List<String> att = new ArrayList<String>();
			att.add(new Integer(category).toString());
    		att.add(new Integer(dataId).toString());
    		att.add(new Integer(i).toString());
    		
    		List<String> prim = new ArrayList<String>();
    		for (int c : newPrimaryKey) {
	    		prim.add(c + "");
    		}
    		
    		key = Key.createKey(prim, att);
    		
    		if (i < nbInt)
    			operationList.add(operationFactory.createPut(key, Value.createValue(data.getListNumber().get(i).toString().getBytes())));
    		else
    			operationList.add(operationFactory.createPut(key, Value.createValue(data.getListString().get(i - nbInt).toString().getBytes())));
		}
		
		return operationList;
	}
	
	/**
	 * get the transaction matching the operation. if new primary key is set, shift keys
	 * @param data
	 * @param newPrimaryKey
	 * @return
	 */
	private List<oracle.kv.Operation> getRemoveDataTransaction (Data data, List<Integer> newPrimaryKey) {
		int dataId = data.getId();
		int category = data.getCategory();
		Key key;
		OperationFactory operationFactory = store.getOperationFactory();
		List<oracle.kv.Operation> operationList = new ArrayList<oracle.kv.Operation>();
		
		for (int i = 0; i < nbInt + nbString; i++) {
			List<String> att = new ArrayList<String>();
			att.add(new Integer(category).toString());
    		att.add(new Integer(dataId).toString());
    		att.add(new Integer(i).toString());
    		
    		List<String> prim = new ArrayList<String>();
    		for (int c : newPrimaryKey) {
	    		prim.add(c + "");
    		}
    		
    		key = Key.createKey(prim, att);
    		
    		operationList.add(operationFactory.createDelete(key));
		}
		
		return operationList;
	}
	
	
	private List<Data> getAllDataFromProfile(int profile) {
		List<Data> datas = new ArrayList<Data>();

		SortedMap<Key,ValueVersion> profileObjects = store.multiGet(Key.createKey("" + profile), null, null);

		int i = 0;
		Data data = new Data();
		for (Entry<Key, ValueVersion> profileObject : profileObjects.entrySet()) {
			String value = new String (profileObject.getValue().getValue().getValue());
			if (i < nbInt)
				data.getListNumber().add(Integer.valueOf(value));
    		else
    			data.getListString().add(value);
    		i++;
    		
    		if (i == (nbInt + nbString)) {
				i = 0;
				data.setId(Integer.valueOf(profileObject.getKey().getMinorPath().get(0)));
				data.setCategory(Integer.valueOf(profileObject.getKey().getMajorPath().get(0)));
				datas.add(data);
				data = new Data();
    		}
		}
		
		return datas;
	}
	
	
	/**
	 * transfuse to target given profiles
	 * @param localProfiles
	 * @param target
	 * @throws RemoteException 
	 */
	@Override
	public void transfuseData(int profile, KVDBInterface target) throws RemoteException {
		List<Data> unusedData = new ArrayList<Data>();
		unusedData.addAll(getAllDataFromProfile(profile));
		////System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!unused data size = " + unusedData.size());
		////profileMutexes.get(profile).writeLock().lock();
		
		if (unusedData.size() != 0) {
			//data 2 transaction
			List<Operation> transfuseOperations = new ArrayList<Operation>();
			for (Data data : unusedData) {
				transfuseOperations.add(new WriteOperation(data));
			}
		
			try {
				//add them to target (inject)
				target.executeOperations(transfuseOperations);
			}catch (Exception e) {
				//System.out.println(id + " Exception while transfusing to target, the profile " + profile);
				e.printStackTrace();
			}
			
			//remove them from here (delete)
			List<Operation> deleteOperations = new ArrayList<Operation>();
			for (Data data : unusedData) {
				deleteOperations.add(new DeleteOperation(data));
			}
			executeOperations(deleteOperations);
		}
		
		//System.out.println(id + " transfused profile " + profile + " to target " + target.getKVDBId() + ", successful");
		localProfiles.remove(profile);
		//profileMutexes.remove(profile);
	}
	
	
	/**
	 * ask migration from kvdb having profiles v to me
	 * @param profiles
	 * @throws RemoteException 
	 */
	//TODO make it more efficient
	private void migrate(List<Integer> profiles, boolean isLoadBalancing) {
		List<KVDBInterface> targetServers = new ArrayList<KVDBInterface>();
		
		//ask migration of the list of profile to me
		for (Integer profile : profiles) {
			try {
				if (isLoadBalancing)
					targetServers.add(monitorMapping.get(profile).notifyLoadBalanceMigration(this, profile));
				else
					targetServers.add(monitorMapping.get(profile).notifyStandardMigration(this, profile));
			} catch (RemoteException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			//create mutex because of the future injection
			//profileMutexes.put(profile, new ReentrantReadWriteLock(true));
		}
		
		//begin migration of profiles to me
		for (int i = 0; i < profiles.size(); i++) {
			int profile = profiles.get(i);
			//System.out.println(id + " migrating " + profile);
			KVDBInterface kvdb = targetServers.get(i);
			try {
				if (kvdb.getKVDBId() == this.id) {
					//System.out.println(id + " already having profile");
					continue;
				}
				//System.out.println("targetServer having " + profile + " = " + kvdb.getKVDBId());
				kvdb.transfuseData(profile, this);
			} catch (RemoteException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			localProfiles.put(profile, profile);
		}
		
		//end migration
		for (Integer profile : profiles) {
			try {
				if (isLoadBalancing)
					monitorMapping.get(profile).notifyEndLoadBalanceMigration(this, profile);
				else
					monitorMapping.get(profile).notifyEndStandardMigration(this, profile);
			} catch (RemoteException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	
	
	@Override
	public void startDB() {
		loadBalancer = new Thread(new Runnable() {
			private boolean hasThrownToken = false;
			
			private void throwToken() {
					tokens.put(id, new SleepingToken(id, KVDBImplementation.this));
					hasThrownToken = true;
			}
			
			private boolean hasLowLoad() {
				if (id == 5)
					return true;
				return false;
			}
			
			private boolean hasHighLoad() {
				if ((id == 0) && localProfiles.size() != 0)
					return true;
				return false;
			}
			
			private List<Integer> getLowUsedProfiles() {
				List<Integer> result = new ArrayList<Integer>();
				synchronized (localProfiles) {
					if (localProfiles.size() != 0) {
						result.add(localProfiles.keySet().iterator().next());
					}
				}
				
				return result;
			}
			
			@Override
			public void run() {
				while (runLoadBalancer) {
					//check if we are busy, in this case, we consume the first token
					if ((tokens.size() != 0) && hasHighLoad()) {
						//System.out.println(KVDBImplementation.this.getKVDBId() + " tokens size = " + tokens.size());
						int tokenIndex = tokens.entrySet().iterator().next().getKey();
						TokenInterface token = tokens.remove(tokenIndex);
						List<Integer> lowUsedProfiles = getLowUsedProfiles();
						
						token.getProfiles().addAll(lowUsedProfiles);
						//System.out.println(KVDBImplementation.this.getKVDBId() + " is busy, consuming token " + token.getId() + " to give " + token.getProfiles());
						try {
							token.getKvdb().sendToken(token);
						} catch (RemoteException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					
					
					//if we have thrown our token, check if it's there to steal some jobs
					if (hasThrownToken) {
						if (tokens.containsKey(id)) {
							//System.out.println(KVDBImplementation.this.id + " mon bébé :)");
							TokenInterface token = tokens.get(id);
							tokens.remove(token);
							hasThrownToken = false;
							if (! hasHighLoad() && (token.getProfiles().size() != 0)) {
								migrate(token.getProfiles(), true);
							}
							token.setProfiles(new ArrayList<Integer>());
						}
					}
					
					//if we detect that we do nothing, throw a sleeping token
					if ((! hasThrownToken) && hasLowLoad()) {
						////System.out.println(KVDB.this.id + " throwing a token his sleeping token");
						throwToken();
					}
					
					if (tokens.size() != 0) {
						//we send to our neighbours the rest of tokens
						try {
							neighbourKvdbs.get("right").sendToken(tokens);
						} catch (RemoteException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						tokens.clear();
					}
					
					/*try {
						Thread.sleep(500);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}*/
				}
				
			}
		});
		loadBalancer.start();
	}
	
	
	@Override
	public void sendToken(Map<Integer, TokenInterface> tokens) {
		this.tokens.putAll(tokens);
	}
	
	@Override
	public void sendToken(TokenInterface token) {
		this.tokens.put(token.getId(), token);
	}
	
	
	
	@Override
	public void printDB() {
        //foreach profile
		for (Integer profile : localProfiles.keySet()) {
			SortedMap<Key,ValueVersion> profileObjects = store.multiGet(Key.createKey(profile + ""), null, null);
			
			for (Entry<Key, ValueVersion> profileObject : profileObjects.entrySet()) {
				ValueVersion valueVersion = profileObject.getValue();
				Key key = profileObject.getKey();
				
				//System.out.println("id = " + id + " clé = " + key + ", valeur = " + new String(valueVersion.getValue().getValue()));
			}
		}
    }
	
	@Override
	public void closeDB() {
		/*for (Integer profile : this.profiles) {
			store.multiDelete(Key.createKey("" + profile), null, null);
		}*/
		runLoadBalancer = false;
		if (loadBalancer != null) {
			try {
				loadBalancer.join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		//System.out.println(id + "--------------------------------ok");
		//store.close();
	}
	

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + id;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		KVDBImplementation other = (KVDBImplementation) obj;
		if (id != other.id)
			return false;
		return true;
	}
	
	@Override
	public String toString() {
		return "KVDB [id=" + id + ", storeName=" + storeName + ", profiles="
				+ localProfiles + "]";
	}
}
