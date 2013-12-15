package db;

import java.util.ArrayList;
import java.util.List;

import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.Value;
import oracle.kv.ValueVersion;
import transaction.Operation;
import transaction.OperationResult;

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
    private String storeName = "kvstore";
    private String hostName = "localhost";
    private String hostPort = "5000";
    private int id;
    
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
	
	
	private void initBase() {
        String profile = "P";
        String object = "O";
        String attribute = "A";
        Key key;
        
        //instanciation de la base de donnée
		try {
			//TODO lire le fichier de conf pour avoir les info de configuration
		      store = KVStoreFactory.getStore(new KVStoreConfig(storeName, hostName + ":" + hostPort));
		} catch (Exception e) {
		    e.printStackTrace();
		}
        
        //foreach profile
        for (int i = id; i < (id + 5); i++) {
        	profiles.add(profile + i);
        	//foreach object
            for (int j = 0; j < 7; j++) {
            	//foreach attribute
            	for (int k = 0; k < 10; k++) {
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
		List<OperationResult> result = null;
		
		//convert operations to kvstore operations
		
		//1 thread par execution
		
		//if the operation has multiple key : 
			//fetch tables
			//convert them to one category
			//execute transaction
			//retransform to multiple key
		
		
		return result;
	}
	
	private List<oracle.kv.Operation> convertOperations (List<Operation> operations) {
		return null;
	}
	
	
	
	/**thread qui doit passer les jetons dans un anneau
		si moi même je ne fais rien, j'envoi un jeton qui doit faire le tour
			si les autres sont surchargés, le premier qui recoit mon jeton m'envoie les tables sur lequel il ne travaille pas trop (le surchargé garde les gros)
			je détecte qu'on menvoie du travail en regardant mon jeton recut : 
			 	- si jeton vide, alors il a faits le tour de l'anneau
			 	- sinon, on m'envoiee du travail
	*/
	
	
	
	public void printDB() {
		Key key;
        
        String object = "O";
        String attribute = "A";
        //foreach profile
		for (String profile : profiles) {
			//foreach object
			for (int j = 0; j < 7; j++) {
				//foreach attribute
		    	for (int k = 0; k < 10; k++) {
		    		List<String> att = new ArrayList<String>();
		    		att.add(object + new Integer(j).toString());
		    		att.add(attribute + new Integer(j).toString());
		    		key = Key.createKey(profile, att);
		    		ValueVersion valueVersion = store.get(key);
	            	System.out.println("id = " + id + " clé = " + key + ", valeur = " + new String(valueVersion.getValue().getValue()));
		    	}
		    }
		}
    }
	
	public void closeDB() {
		store.close();
	}
}
