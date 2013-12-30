package monitor;

import java.util.List;

import transaction.OperationResult;
import transaction.Operation;

public interface MonitorInterface {
	//client
	List<OperationResult> executeOperations(List<Operation> operations);
	
	//kvdb notifier les migrations
	void notifyMigration ();
	// kvdb notifie fin de migration
	void notifyEndMigration();
	
}
