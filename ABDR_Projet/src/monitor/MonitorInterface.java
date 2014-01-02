package monitor;

import java.util.List;

import db.KVDB;
import transaction.OperationResult;
import transaction.Operation;

public interface MonitorInterface {
	//client
	List<OperationResult> executeOperations(List<Operation> operations);
	
	//kvdb notifier les migrations
	KVDB notifyMigration (KVDB newSource, int profile);
	
	// kvdb notifie fin de migration
	void notifyEndMigration(KVDB newSource, int profile);
	
}
