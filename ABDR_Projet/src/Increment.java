
import oracle.kv.*;
import oracle.kv.stats.*;

/**
 * TME avec KVStore : Init
 */
public class Increment{
	private static KVStore store;
    
    private static String storeName = "kvstore";
    private static String hostName = "localhost";
    private static String hostPort = "5000";
    
    
    public static void initBase(KVStore store) {
        String k = "P";
        String value = "0";
        Key key;
        
        for (int i = 0; i < 100; i++) {
            key = Key.createKey(k + i);
            store.put(key, Value.createValue(value.getBytes()));
        }
    }
    
    
    private static void incrAll(KVStore store) {
        String k = "P";
        ValueVersion valueVersion;
        Key key = Key.createKey(k + 1);

        for (int i = 0; i < 1000; i++) {
            valueVersion = store.get(key);
            int curr = Integer.parseInt(new String(valueVersion.getValue().getValue()));
            curr++;
            
            store.put(key, Value.createValue((curr + "").getBytes()));
            
            System.out.println("clé = " + k + 1 + ", valeur = " + new String(valueVersion.getValue().getValue()));
            
        }
    }
    
    private static void incrAll2(KVStore store) {
        String k = "P";
        ValueVersion valueVersion;
        Key key = Key.createKey(k + 1);
        Version version;

        for (int i = 0; i < 1000; i++) {
            valueVersion = store.get(key);
            int curr = Integer.parseInt(new String(valueVersion.getValue().getValue()));
            curr++;
            
            version = store.putIfVersion(key, Value.createValue((curr + "").getBytes()), valueVersion.getVersion());
            
            if (version == null)
            	i--;
            
            System.out.println("clé = " + k + 1 + ", valeur = " + new String(valueVersion.getValue().getValue()));
        }
    }
    
	
	public static void main(String args[]) {
        try {
            store = KVStoreFactory.getStore(new KVStoreConfig(storeName, hostName + ":" + hostPort));
            //initBase(store);
            //incrAll(store);
            incrAll2(store);
            store.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
