package monitor;

import java.util.List;

import transaction.OperationResult;
import transaction.Operation;

public interface MonitorInterface {
	OperationResult executeOperations(List<Operation> operations);
}
