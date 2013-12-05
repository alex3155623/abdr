package monitor;

import java.util.ArrayList;
import java.util.List;

import transaction.Operation;
import transaction.OperationResult;
import db.KVDB;

public class Monitor implements MonitorInterface{
	private List<KVDB> servers = new ArrayList<KVDB>();

	@Override
	public OperationResult executeOperations(List<Operation> operations) {
		// TODO Auto-generated method stub
		return null;
	}
}
