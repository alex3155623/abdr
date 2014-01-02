package db;

import java.util.List;

import transaction.Data;
import transaction.Operation;
import transaction.OperationResult;

public interface KVDBInterface {
	//accept user transactions
	List<OperationResult> executeOperations(List<Operation> operations);
	
	
	//interface to handle load balancing
	//void créer jeton je ne fais rien et l'envoyer
	//void attendre les jetons
		//- le jeton contient les tables associés
	
	void transfuseData(List<Integer>profiles, KVDB target);
	void injectData(List<Operation> data);
	
	void closeDB();
	
	void printDB();
	
}