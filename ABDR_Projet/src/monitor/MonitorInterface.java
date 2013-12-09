package monitor;

import java.util.List;

import transaction.OperationResult;
import transaction.Operation;

public interface MonitorInterface {
	//client
	OperationResult executeOperations(List<Operation> operations);
	
	//kvdb notifier les migrations
	void notifyMigration (int idDest, List<String> products);
	
	
}
