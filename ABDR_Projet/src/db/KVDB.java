package db;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;

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
import transaction.Operation;
import transaction.OperationResult;
import transaction.ReadOperation;
import transaction.WriteOperation;

/** 
 * gestion d'une base de donnée
 * execution des transactions, va gérer la migration
 * 	- convertissement des clés principales pour avoir une clé principale unique pour la transaction
 * 	- tu fais la transaction
 * 	- une fois que c'est fait, on retransforme les catégories tel qu'ils étaient
 * 
 * 	- répartion de type anneau, + notification au moniteur des migrations
 * 
 * transformer = multiget, transaction write...
 * 
 * @author 2600705
 *
 */

public class KVDB implements DBInterface {
	private KVStore store;
	private int id;
    private String storeName = "kvstore";
    private String hostName = "localhost";
    private String hostPort = "5000";
    
    private final String profile = "P";
    private final String object = "O";
    private final String attribute = "A";
    
    private final int nbInt = 5;
    private final int nbString = 5;
    private final int nbObjects = 5;
    private final int nbProfile = 5;

    private List<KVDB> otherDBs = new ArrayList<KVDB>();
    private List<String> profiles = new ArrayList<String>();
    
    
	public KVDB() {
		initBase();
	}
	
	public KVDB(int id) {
		this.id = id;
		initBase();
	}
	
	
	public KVDB(int id, String storeName, String hostName, String hostPort) {
		this.id = id;
		this.storeName = storeName;
		this.hostName = hostName;
		this.hostPort = hostPort;
		initBase();
	}
	
	@Override
	public List<String> getProfiles(){ return profiles;}
	
	private void initBase() {
        Key key;

        //instanciation de la base de donnée
		try {
			//TODO lire le fichier de conf pour avoir les info de configuration
		      store = KVStoreFactory.getStore(new KVStoreConfig(storeName, hostName + ":" + hostPort));
		} catch (Exception e) {
		    e.printStackTrace();
		}
        
        //foreach profile
       for (int i = id * nbProfile; i < (id * nbProfile) + nbProfile; i++) {
        	profiles.add(profile + i);
        	//foreach object
            for (int j = 0; j < nbObjects; j++) {
            	//foreach attribute
            	for (int k = 0; k < nbString + nbInt; k++) {
            		List<String> att = new ArrayList<String>();
            		att.add(object + new Integer(j).toString());
            		att.add(attribute + new Integer(k).toString());
            		key = Key.createKey(profile + i,  att);
            		store.put(key, Value.createValue(new Integer(0).toString().getBytes()));
            	}
            }
        }
    }
	
	
	@Override
	public List<OperationResult> executeOperations(List<Operation> operations) {
		//synchronized, verroux lecteur ecrivain
		//1 verroux lecteur ecrivain par catégorie 
			//-> pourri les perf, rend sequentiel des acces potentiellement concurrent
		//1 verroux par objet
			//bof
		
		
		List<OperationResult> result = new ArrayList<OperationResult>();
		
		//si c'est un simple get, on fait un multiget sur la clé
		if ((operations.size() == 1) && (operations.get(0) instanceof ReadOperation)) {
			result = getData(operations.get(0).getData());
		}
		else {
			//sinon, c'est une transaction
			
			//if the operation has multiple key : 
			//fetch tables
			//convert them to one category
			//execute transaction
			//retransform to multiple key
			List<oracle.kv.Operation> opList = convertOperations(operations);
			
		    try {
				List<oracle.kv.OperationResult> res = store.execute(opList);
				result.addAll(KVResult2OperationResult(res));
				
				
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
		}
		
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

		SortedMap<Key,ValueVersion> profileObjects = store.multiGet(Key.createKey(profile + category), new KeyRange (object + new Integer(dataId).toString()), null);

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
	
	
	private List<oracle.kv.Operation> convertOperations (List<Operation> operations) {
		List<oracle.kv.Operation> operationList = new ArrayList<oracle.kv.Operation>();

		for (Operation operation : operations) {
			if (operation instanceof WriteOperation) {
				operationList.addAll(addData(operation.getData()));
			}
			else {
				operationList.addAll(removeData(operation.getData()));
			}
		}
		
		return operationList;
	}
	
	
	private void transfuseData(List<String>profiles, KVDB target) {
		for (String profile : profiles) {
			//data 2 object
			
			//add them to target
			
			//remove them from here
			
			/**
			 * 
			 * 
			 */
		}
	}
	
	/**
	 * Create a list of operations to add a data object
	 * @param data
	 * @return
	 */
	private List<oracle.kv.Operation> addData (Data data) {
		int dataId = data.getId();
		int category = data.getCategory();
		Key key;
		OperationFactory operationFactory = store.getOperationFactory();
		List<oracle.kv.Operation> operationList = new ArrayList<oracle.kv.Operation>();
		
		if (! profiles.contains(profile + category))
			profiles.add (profile + category);
		
		for (int i = 0; i < nbInt + nbString; i++) {
			List<String> att = new ArrayList<String>();
    		att.add(object + new Integer(dataId).toString());
    		att.add(attribute + new Integer(i).toString());
    		key = Key.createKey(profile + category, att);
    		
    		if (i < nbInt)
    			operationList.add(operationFactory.createPut(key, Value.createValue(data.getListNumber().get(i).toString().getBytes())));
    		else
    			operationList.add(operationFactory.createPut(key, Value.createValue(data.getListString().get(i - nbInt).toString().getBytes())));
		}
		
		return operationList;
	}
	
	/**
	 * Create a list of operations to delete a data object
	 * @param data
	 * @return
	 */
	private List<oracle.kv.Operation> removeData (Data data) {
		int dataId = data.getId();
		int category = data.getCategory();
		Key key;
		OperationFactory operationFactory = store.getOperationFactory();
		List<oracle.kv.Operation> operationList = new ArrayList<oracle.kv.Operation>();
		
		if (! profiles.contains(profile + category))
			profiles.add (profile + category);
		
		for (int i = 0; i < nbInt + nbString; i++) {
			List<String> att = new ArrayList<String>();
    		att.add(object + new Integer(dataId).toString());
    		att.add(attribute + new Integer(i).toString());
    		key = Key.createKey(profile + category, att);
    		operationList.add(operationFactory.createDelete(key));
		}
		
		return operationList;
	}
	
	
	/**thread qui doit passer les jetons dans un anneau
		si moi même je ne fais rien, j'envoi un jeton qui doit faire le tour
			si les autres sont surchargés, le premier qui recoit mon jeton m'envoie les tables sur lequel il ne travaille pas trop (le surchargé garde les gros)
			je détecte qu'on menvoie du travail en regardant mon jeton recut : 
			 	- si jeton vide, alors il a faits le tour de l'anneau
			 	- sinon, on m'envoiee du travail
	*/
	
	@Override
	public void injectData(List<Data> data) {
		// TODO Auto-generated method stub
		
	}
	
	//TODO remove
	public void addNeighbor(KVDB newDB) {
		otherDBs.add(newDB);
	}
	
	@Override
	public void printDB() {
        //foreach profile
		for (String profile : profiles) {
			SortedMap<Key,ValueVersion> profileObjects = store.multiGet(Key.createKey(profile), null, null);
			
			for (Entry<Key, ValueVersion> profileObject : profileObjects.entrySet()) {
				ValueVersion valueVersion = profileObject.getValue();
				Key key = profileObject.getKey();
				
				System.out.println("id = " + id + " clé = " + key + ", valeur = " + new String(valueVersion.getValue().getValue()));
			}
		}
    }
	
	@Override
	public void closeDB() {
		store.close();
	}
}
