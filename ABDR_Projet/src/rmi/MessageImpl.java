package rmi;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class MessageImpl extends UnicastRemoteObject implements Msg {

    public MessageImpl() throws RemoteException {        
    }
    
    @Override
    public void sayHello(String name) throws RemoteException {
        System.out.println("hello "+name);
    }
    
}
