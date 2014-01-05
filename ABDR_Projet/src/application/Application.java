package application;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

import transaction.Data;
import transaction.DeleteOperation;
import transaction.Operation;
import transaction.OperationResult;
import transaction.ReadOperation;
import transaction.WriteOperation;
import monitor.Monitor;

//envoyer des transactions (ex : 100 write dans une catégorie) pendant 10 secondes
//on suppose qu'on connait le moniteur
public class Application implements Runnable {
	private List<Integer> targetProfiles;
	private Map<Integer, Monitor> monitors;
	private long duration;
	private int nbIter = 0;
	private int shift;
	private List<Long> myTimes = new ArrayList<Long>();
	
	private static int globalId = 100000;
	private static Semaphore mutex = new Semaphore(1);
	
	public Application(List<Integer> targetProfiles, Map<Integer, Monitor> monitors, long duration, int shift) {
		this.targetProfiles = targetProfiles;
		this.monitors = monitors;
		this.duration = duration;
		this.shift = shift;
	}
	
	
	/**
	 * Lancement de l'application
	 * @param monitor
	 */
	public void startApp() {
		long startTime =  System.currentTimeMillis();
		int currentId = shift;
		
		// L'appli itère pendant 10 secondes
		// Opération d'écriture de 100 objets sur un Pi
		while(System.currentTimeMillis() - startTime < duration * 1000){
			
			long begin = System.currentTimeMillis();
			// Création de 10 objets data avec 5 integer et 5 string 
			List<Operation> operations = new ArrayList<Operation>();
			for(int i=0;i<100;i++){
				for (int profile : targetProfiles) {
					Data d = new Data();
					
					try {
						Application.mutex.acquire();
						d.setId(currentId);
						currentId++;
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} finally {
						Application.mutex.release();
					}
					
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
				//System.out.println("inserting " + operations.get(0).getData().getCategory() + ", with id " + operations.get(0).getData().getId());
				// On demande au monitor d'executer les opérations
			}
			System.out.println("tentative transaction");
			monitors.get(targetProfiles.get(0)).executeOperations(operations);
			long end = System.currentTimeMillis();
			myTimes.add(end - begin);
			System.out.println("insert ok " + nbIter);
			
			nbIter++;
		}
		
		long total = 0;
		for (long curr : myTimes) {
			total += curr;
		}
		total /= nbIter;
		
		System.out.println("total : " + total);
	}
	
	public void accessData() {
		/*long startTime =  System.currentTimeMillis();
		int currentId = shift;
		
		// L'appli itère pendant 10 secondes
		// Opération d'écriture de 100 objets sur un Pi
		//while(System.currentTimeMillis() - startTime < duration * 1000){
			
			// Création de 10 objets data avec 5 integer et 5 string 
			for(int i=0;i<20;i++){
				List<Operation> operations = new ArrayList<Operation>();
				int profile = targetProfiles.get(0);
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
				operations.add(new ReadOperation(d));
			
				//System.out.println("reading " + operations.get(0).getData().getCategory() + ", with id " + operations.get(0).getData().getId());
				//long begin = System.currentTimeMillis();
				monitors.get(targetProfiles.get(0)).executeOperations(operations);
				//long end = System.currentTimeMillis();
				//System.out.println("--------------------------time of read = " + (end - begin));
				currentId++;
			}*/
			// On demande au monitor d'executer les opérations
			
		//}
	}
	
	
	public void check() {
		int currentId = shift;
		for (int k = 0; k < nbIter; k++) {
			// Création de 10 objets data avec 5 integer et 5 string 
			for(int i=0;i<20;i++) {
				List<Operation> operations = new ArrayList<Operation>();
				for (int profile : targetProfiles) {
					Data d = new Data();
					
					try {
						Application.mutex.acquire();
						currentId--;
						d.setId(currentId);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} finally {
						Application.mutex.release();
					}
					
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
					operations.add(new DeleteOperation(d));
				}
				//System.out.println("deleting " + operations.get(0).getData().getCategory() + ", with id " + operations.get(0).getData().getId());
				
				// On demande au monitor d'executer les opérations
				List<OperationResult> res = monitors.get(targetProfiles.get(0)).executeOperations(operations);
				
				for (OperationResult op : res) {
					if (! op.isSuccess())
						throw new IllegalStateException("fuuuuuuuuuuuuuuuuuuuu");
				}

				currentId++;
			}
		}
	}


	@Override
	public void run() {
		startApp();
		//accessData();
		//check();
		System.out.println(" +++++++++++++++++++++++++++ end application");
	}

	

}
