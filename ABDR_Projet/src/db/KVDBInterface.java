package db;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Map;

import monitor.MonitorInterface;
import transaction.Operation;
import transaction.OperationResult;

public interface KVDBInterface extends Remote {
	
	
	//accept user transactions
	List<OperationResult> executeOperations(List<Operation> operations) throws RemoteException;
	
	int getKVDBId() throws RemoteException;
	
	void transfuseData(int profile, KVDBInterface target) throws RemoteException;
	
	void startLoadBalance() throws RemoteException;
	
	void closeDB() throws RemoteException;
	
	void printDB() throws RemoteException;
	
	/************************/
	void setLeftKVDB(KVDBInterface kvdbLeft) throws RemoteException;
	void setRightKVDB(KVDBInterface kvdbRight) throws RemoteException;
	void setMonitors(Map<Integer, MonitorInterface> monitors) throws RemoteException;
	void setSelf(KVDBInterface myself) throws RemoteException;
	
	
	void sendToken(Map<Integer, TokenInterface> tokens) throws RemoteException;
	void sendToken(TokenInterface token) throws RemoteException;
}
