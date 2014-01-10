package rmi;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class S1 {

    public static void main(String[] args) {
	if(args.length==0) {
	    System.out.println("ajouter le nom de la machine en parametre");
	    System.exit(0);
	}
	if(args[0].equals("localhost")) {
	    System.out.println("ajouter le nom reel de la machine, pas localhost");
	    System.exit(0);
	}
	String servername=args[0];
        S1 s = new S1();
        s.startServer(servername);
    }
    
    private void startServer(String servername){
        try {
	    // le serveur doit redire le nom du server, cela sert pour le loopback
	    System.setProperty("java.rmi.server.hostname", servername); 

            // create on port 1099
            Registry registry = LocateRegistry.createRegistry(1099);
            
            // create a new service named myMessage
            registry.rebind("myMessage", new MessageImpl());
        } catch (Exception e) {
            e.printStackTrace();
        }      
        System.out.println("system is ready");
    }
    
}
