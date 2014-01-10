package monitor;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

import db.KVDBInterface;
import transaction.OperationResult;
import transaction.Operation;

public interface MonitorInterface extends Remote {
	//client
	List<OperationResult> executeOperations(List<Operation> operations) throws RemoteException;
	
	// kvdb notifier les migrations
	KVDBInterface notifyLoadBalanceMigration (KVDBInterface newSource, int profile) throws RemoteException;
	
	// kvdb notifie fin de migration
	void notifyEndLoadBalanceMigration(KVDBInterface newSource, int profile) throws RemoteException;
	
	// kvdb notifie de temporairement de perdre le read, le temps de la migration
	KVDBInterface notifyStandardMigration(KVDBInterface newSource, int profile) throws RemoteException;
	
	// kvdb notifie la reprise du read
	void notifyEndStandardMigration(KVDBInterface newSource, int profile) throws RemoteException;
}
