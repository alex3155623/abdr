package application;

import java.util.ArrayList;
import java.util.List;

import transaction.Data;
import transaction.Operation;
import transaction.WriteOperation;
import monitor.Monitor;

//envoyer des transactions (ex : 100 write dans une catégorie) pendant 10 secondes
//on suppose qu'on connait le moniteur
public class Application {

	/**
	 * Lancement de l'application
	 * @param monitor
	 */
	public static void start(Monitor monitor) {
		long startTime =  System.currentTimeMillis();
		int category = 1;
		// L'appli it�re pendant 10 secondes
		// Op�ration d'�criture de 100 objets sur un Pi
		while(System.currentTimeMillis()-startTime < 10000){
			List<Operation> operations = new ArrayList<Operation>();
			// Cr�ation de 100 objets data avec 5 integer et 5 string 
			for(int i=0;i<100;i++){
				Data d = new Data();
				d.setId(i);
				d.setCategory(category);
				List<Integer> listNumber = new ArrayList<Integer>();
				List<String> listString = new ArrayList<String>();
				for(int j =0; j<5 ; j++)
				{
					listNumber.add(j);
					listString.add("test"+j);
				}
				d.setListNumber(listNumber);
				d.setListString(listString);
				
				// On l'ajoute � la liste des op�rations
				operations.add(new WriteOperation(d));
			}
			// On demande au monitor d'executer les op�rations
			monitor.executeOperations(operations);
			category++;
		}
		
	}

	

}
