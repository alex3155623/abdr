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
//dadaad
public class Monitor implements MonitorInterface{
	/**
	 * Server List
	 */
	private List<KVDB> servers = new ArrayList<KVDB>();

	/**
	 * Monitor Constructor 
	 * @param kvdbs
	 */
	public Monitor(List<KVDB> kvdbs) {
		// TODO Auto-generated constructor stub
		servers = kvdbs;
	}

	/**
	 *  Execute a list of operation
	 */
	@Override
	public OperationResult executeOperations(List<Operation> operations) {
	
		findProfile(operations);
		// search the good kvstore
		KVDB store = findKVDB(operations);
		List<OperationResult> listOpR = store.executeOperations(operations);
		for(OperationResult or :listOpR)
		{
			if(!or.isSuccess())
				return or;
		}
		//  A VOIR POUR LA VALEUR DE RETOUR
		return null;
	}

	private int findProfile(List<Operation> operations) {
		int tmp = 0;
		int profile = 0;
		for(Operation op : operations){
			tmp = op.getData().getCategory();
			if(tmp == profile || profile==0)
				profile = tmp;
			else
				// Plusieurs profile touch� dans la transaction;
				;
		}
		
		return profile;
		
	}
	
	private KVDB findKVDB(List<Operation> operations){
		for (KVDB k : servers){
			if(k.getProfiles().contains("P"+findProfile(operations)))
				  return k;
		}
		return null;
	}

	@Override
	public void notifyMigration(int idDest, List<String> products) {
		// TODO Auto-generated method stub
		
	}
}
