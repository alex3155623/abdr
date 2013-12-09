package db;

import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.Value;
import oracle.kv.ValueVersion;

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

public class KVDB {
	private KVStore store;
    private String storeName = "kvstore";
    private String hostName = "localhost";
    private String hostPort = "5000";
    private int id;
    
	public KVDB() {
		//instanciation de la base de donnée
		try {
			//TODO lire le fichier de conf
			
			
			
			
            store = KVStoreFactory.getStore(new KVStoreConfig(storeName, hostName + ":" + hostPort));
            initBase(store);
            //dadada
            //printDB(store);
            store.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
	}
	
	private void initBase(KVStore store) {
        String profile = "P";
        String object = "O";
        String attribute = "A";
        Key key;
        
        //foreach profile
        /*for (int i = 0; i < 10; i++) {
        	//foreach object
            for (int j = 1; j < 21; j++) {
            	key = Key.createKey(categorie + (i + 1), produit + (j + (20 * i)));
            	store.put(key, Value.createValue(value.getBytes()));
            }
        }*/
    }
	
	private void printDB(KVStore store) {
    	String value = "1";
        String categorie = "C";
        String produit = "P";
        Key key;
        
    	//foreach categorie
        for (int i = 0; i < 10; i++) {
        	//foreach product
            for (int j = 1; j < 21; j++) {
            	key = Key.createKey(categorie + (i + 1), produit + (j + (20 * i)));
            	ValueVersion valueVersion = store.get(key);
            	System.out.println("clé = " + key + ", valeur = " + new String(valueVersion.getValue().getValue()));
            }
        }
    }
}
