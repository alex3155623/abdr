package rmi;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class C1 {
    public static void main(String[] args) {
	if(args.length==0) {
	    System.out.println("ajouter le nom de la machine en parametre");
	    System.exit(0);
	}
	String servername=args[0];

        C1 c = new C1();	
        c.doTest(servername);
    }
    
    private void doTest(String servername){
        try {
	    // fire to server host on port 1099
            Registry myRegistry = LocateRegistry.getRegistry(servername, 1099);
			
	    // search for myMessage service
            Msg impl = (Msg) myRegistry.lookup("myMessage");
		


	
	    // call server's method			
            impl.sayHello("edwin");
			
            System.out.println("Message Sent");
        } catch (Exception e) {
            e.printStackTrace();
        }        
    }
    
}
