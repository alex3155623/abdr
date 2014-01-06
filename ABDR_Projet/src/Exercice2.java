import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;

import oracle.kv.DurabilityException;
import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.Operation;
import oracle.kv.OperationExecutionException;
import oracle.kv.OperationFactory;
import oracle.kv.ReturnValueVersion;
import oracle.kv.Value;
import oracle.kv.ValueVersion;
import oracle.kv.Version;

//Avro formatter
/**
 * Exercice TME kvstore
 *
 */
public class Exercice2 {
	
	/**
	 * Base de donnee kvstore
	 */
	private static KVStore store;
    /**
     * Nom de la base
     */
    private static String storeName = "kvstore";
    /**
     * Nom de l'host : localhost si en local
     */
    private static String hostName = "localhost";
    /**
     * Numéro du port utilisé
     */
    private static String hostPort = "5000";
    
    
    /**
     * Peuplement de la base de donnée
     * @param store
     */
    public static void initBase(KVStore store) {
        String value = "1";
        String categorie = "C";
        String produit = "P";
        Key key;

        //foreach categorie
        for (int i = 0; i < 10; i++) {
        	//foreach product
            for (int j = 1; j < 21; j++) {
            	key = Key.createKey(categorie + (i + 1), produit + (j + (20 * i)));
            	store.put(key, Value.createValue(value.getBytes()));
            }
        }
        
      
    }
    
    /**
     * Affichage pour le débuggage
     * @param store
     */
    private static void printDB(KVStore store) {
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
    
    /**
     * Méthode M1
     * @param store
     */
    private static void m1(KVStore store) {
        ValueVersion valueVersion;
        Key key;

        for (int i = 0; i < 1000; i++) {
            //ajout 1 à la quantité p1 à p5 de la catégorie c1
        	for (int j = 1; j < 6; j++) {
        		key = Key.createKey("C1", "P" + j);
        		valueVersion = store.get(key);
        		int quantity = Integer.parseInt(new String(valueVersion.getValue().getValue()));
        		quantity++;
        		
        		store.put(key, Value.createValue((""+quantity).getBytes()));
        	}
        }
    }
    
    /**
     * Méthode M2
     * @param store
     */
    private static void m2(KVStore store) {
    	ValueVersion valueVersion;
        Key key;
        int max = 0;

        for (int i = 0; i < 1000; i++) {
        	// de p1 a P6 detecter valeur max
        	for (int j = 1; j < 6; j++) {
        		key = Key.createKey("C1", "P" + j);
        		valueVersion = store.get(key);
        		int quantity = Integer.parseInt(new String(valueVersion.getValue().getValue()));
        		// si max null alors prends la premiere valeur 
        		if (j == 1) {
        			max = quantity;
        			continue;
        		}
        		// Si la quantite est superieur au max, on change la valeur de ce dernier
        		max = (max < quantity) ? quantity : max;
        	}
        	// on ajoute la valeur (max+1) de p1 a P6
        	for (int j = 1; j < 6; j++) {
        		key = Key.createKey("C1", "P" + j);
        		
        		store.put(key, Value.createValue(("" + (max + 1)).getBytes()));
        	}
        }
    }
    
    /**
     * Methode M3
     * cle principale = major component
     * cle secondaire = minor component
     * voir multiget, operation factory
     * transaction = execute (liste operation)
     * @param store
     */
    private static void m3(KVStore store) {
    	Value value;
    	ValueVersion temp;
        Key key;
        int max = 0;
        
        
        //creation d'une transaction
        OperationFactory opFactory = store.getOperationFactory();
        List<Operation> opList = new ArrayList<Operation>();
        List<Version> versionList = new ArrayList<Version>();
        

        for (int j = 0; j < 1000; j++) {
        	opList.clear();
        	versionList.clear();
        	
        	key = Key.createKey("C1");
        	SortedMap<Key,ValueVersion> c1 = store.multiGet(key, null, null);
        	//detection du max + obtention des version des valeurs
        	for (int i = 1; i < 6; i++) {
            	temp = c1.get(Key.createKey("C1", "P" + i));
            	int quantity = Integer.parseInt(new String(temp.getValue().getValue()));
            	versionList.add(temp.getVersion());
            	
        		if (j == 1) {
        			max = quantity;
        			continue;
        		}
        		max = (max < quantity) ? quantity : max;
            }
        	
        	//creation de la liste
        	for (int i = 1; i < 6; i++) {
        		key = Key.createKey("C1", "P" + i);
        		
        		value = Value.createValue(("" + (max + 1)).getBytes());
        		opList.add(opFactory.createPutIfVersion(key, value, versionList.get(i-1), ReturnValueVersion.Choice.VALUE, true));
        	}
        	// execution de la transaction
            try {
				store.execute(opList);
			} catch (DurabilityException e) {
				// TODO Auto-generated catch block
			} catch (OperationExecutionException e) {
				j--;
			} catch (FaultException e) {
				// TODO Auto-generated catch block
			}
        }
    }
    
	
	public static void main(String args[]) {
        try {
            store = KVStoreFactory.getStore(new KVStoreConfig(storeName, hostName + ":" + hostPort));
            //initBase(store);
            //m1(store);
            m2(store);
            //m3(store);
            printDB(store);
            store.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
