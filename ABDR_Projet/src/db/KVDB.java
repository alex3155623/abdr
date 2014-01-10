package db;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;


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

	public static void startKVDB(String hostNameRMI, int portRMI, int id, String storeName, String hostName, String hostPort) {
		try {
			System.setProperty("java.rmi.server.hostname", hostNameRMI);
			Registry registry = LocateRegistry.createRegistry(portRMI);
			
			// create a new service named myMessage
            registry.rebind("KVDB" + id, new KVDBImplementation(id, storeName, hostName, hostPort));
		}
		catch (Exception e) {
			System.out.println("fuuuuuuuuuuu");
			e.printStackTrace();
		}
	}
}
