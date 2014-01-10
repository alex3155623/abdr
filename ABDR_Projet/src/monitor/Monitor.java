package monitor;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.List;
import java.util.Map;

import db.KVDBInterface;

/**
 * aiguilleur + trie la transaction (donne la transaction à la machine ayant la plus petite ressource)
 * donne l'ordre de migration (kvdb migre les table peu utilisées vers une autre db)

	- possède catalogue
 * 
 * @author 2600705
 *
 */
public class Monitor {
	
	public static void startMonitor(Map<Integer, KVDBInterface> kvdbs, int profileOffset, String hostName, int port) {
		try {
			System.setProperty("java.rmi.server.hostname", hostName);
			Registry registry = LocateRegistry.createRegistry(port);
			
			// create a new service named myMessage
            registry.rebind("monitor" + profileOffset, new MonitorImplementation(kvdbs, profileOffset));
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}
