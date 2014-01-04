package application;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import transaction.Data;
import transaction.Operation;
import transaction.WriteOperation;
import monitor.Monitor;

//envoyer des transactions (ex : 100 write dans une catégorie) pendant 10 secondes
//on suppose qu'on connait le moniteur
public class Application implements Runnable {
	private List<Integer> targetProfiles;
	private Map<Integer, Monitor> monitors;
	private long duration;
	
	public Application(List<Integer> targetProfiles, Map<Integer, Monitor> monitors, long duration) {
		this.targetProfiles = targetProfiles;
		this.monitors = monitors;
		this.duration = duration;
	}
	
	
	/**
	 * Lancement de l'application
	 * @param monitor
	 */
	public void start() {
		long startTime =  System.currentTimeMillis();
		int currentId = 0;
		
		targetProfiles.get(0);
		
		// L'appli itère pendant 10 secondes
		// Opération d'écriture de 100 objets sur un Pi
		//while(System.currentTimeMillis() - startTime < duration * 1000){
			List<Operation> operations = new ArrayList<Operation>();
			
			// Création de 10 objets data avec 5 integer et 5 string 
			for(int i=0;i<10;i++){
				for (int profile : targetProfiles) {
					Data d = new Data();
					d.setId(currentId);
					d.setCategory(profile);
					
					List<Integer> listNumber = new ArrayList<Integer>();
					List<String> listString = new ArrayList<String>();
					for(int j =0; j<5 ; j++)
					{
						listNumber.add(j);
						listString.add("test"+j);
					}
					d.setListNumber(listNumber);
					d.setListString(listString);
					
					// On l'ajoute à la liste des opérations
					operations.add(new WriteOperation(d));
				}
				
				currentId++;
			}
			// On demande au monitor d'executer les opérations
			monitors.get(targetProfiles.get(0)).executeOperations(operations);
		//}
	}


	@Override
	public void run() {
		start();
	}

	

}
