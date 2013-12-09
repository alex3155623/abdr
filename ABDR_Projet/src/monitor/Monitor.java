package monitor;

import java.util.ArrayList;
import java.util.List;

import transaction.Operation;
import transaction.OperationResult;
import db.KVDB;

/**
 * aiguilleur + trie la transaction (donne la transaction à la machine ayant la plus petite ressource)
 * donne l'ordre de migration (kvdb migre les table peu utilisées vers une autre db)

	- possède catalogue
 * 
 * @author 2600705
 *
 */

public class Monitor implements MonitorInterface{
	private List<KVDB> servers = new ArrayList<KVDB>();

	@Override
	public OperationResult executeOperations(List<Operation> operations) {
		// TODO Auto-generated method stub
		return null;
	}
}
