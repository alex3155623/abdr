package rmi;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface Msg extends Remote {
    void sayHello(String name) throws RemoteException;
}
