package monitor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
	 * Flag for alert migration
	 */
	private boolean isMigration = false;
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
	public List<OperationResult> executeOperations(List<Operation> operations) {
		
		while(isMigration){
			try{
				wait();
			}catch(InterruptedException ie) {
                ie.printStackTrace();
            }
		}
		//sort the transaction
		operations = sortTransaction(operations);
		
		// search the good kvstore
		KVDB store = findKVDB(operations);
		List<OperationResult> listOpR = store.executeOperations(operations);
		
		return listOpR;
	}

	private List<Operation> sortTransaction(List<Operation> operations) {
		// TODO Auto-generated method stub
		Collections.sort(operations, new Comparator<Operation>() {
			  public int compare(Operation op1, Operation op2) {
			     int data1 = op1.getData().getCategory();
			     int data2 = op2.getData().getCategory();
			     if (data2< data1) return -1;
			     else if(data2== data1 ) return 0;
			     else return 1;
			  }
		});
		
		return operations;

	}

	private ArrayList<String> findProfile(List<Operation> operations) {
		int tmp = 0;
		int profile = 0;
		ArrayList<String> list = new ArrayList<String>();
		for(Operation op : operations){
			tmp = op.getData().getCategory();
			if(tmp == profile || profile==0){
				profile = tmp;
				list.add("P"+profile);
			}
			else{
				// Plusieurs profile touch� dans la transaction;
				list.add("P"+tmp);
			}
		}
		
		return list;
		
	}
	
	private KVDB findKVDB(List<Operation> operations){
		for (KVDB k : servers){
			if(k.getProfiles().contains(findProfile(operations)))
				  return k;
		}
		return null;
	}

	@Override
	public synchronized void notifyMigration() {
		// TODO Auto-generated method stub
		setMigration(true);
	}
	
	public synchronized void notifyEndMigration(){
		setMigration(false);
		notifyAll();
	}

	public boolean isMigration() {
		return isMigration;
	}

	public void setMigration(boolean isMigration) {
		this.isMigration = isMigration;
	}
}
